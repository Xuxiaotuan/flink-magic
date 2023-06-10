package cn.xuyinyin.flink.magic.demo.walkthrough;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

import java.io.IOException;

import static org.xerial.snappy.Snappy.cleanUp;

/**
 * @author : XuJiaWei
 * @since : 2023-06-10 12:18
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    private transient ValueState<Boolean> flageState;
    private transient ValueState<Long> timerState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>("flage", Types.BOOLEAN);
        flageState = getRuntimeContext().getState(flagDescriptor);

        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>("timer-state", Types.LONG);
        timerState = getRuntimeContext().getState(timerDescriptor);
    }

    @Override
    public void processElement(Transaction transaction,
                               KeyedProcessFunction<Long, Transaction,
                                       Alert>.Context context,
                               Collector<Alert> collector) throws Exception {

        // Get the current state for the current key
        Boolean lastTranscationWasSmall = flageState.value();

        // Check if the flag is set
        if (lastTranscationWasSmall != null) {
            if (transaction.getAmount() > LARGE_AMOUNT) {
                // Output an alert downstream
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());

                collector.collect(alert);
            }
            // Clean up our state
            cleanUp(context);
        }

        if (transaction.getAmount() < SMALL_AMOUNT) {
            // set the flag to true
            flageState.update(true);

           long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
           context.timerService().registerProcessingTimeTimer(timer);

           timerState.update(timer);
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Long, Transaction, Alert>.OnTimerContext ctx, Collector<Alert> out) throws Exception {
        // remove flag after 1 minute
        timerState.clear();
        flageState.clear();
    }

    private void cleanUp(Context ctx) throws IOException {
        // delete timer
        Long timer = timerState.value();
        ctx.timerService().deleteEventTimeTimer(timer);

        // clean up all state
        timerState.clear();
        flageState.clear();
    }
}
