/*
From github:
https://github.com/echauchot/tpcds-benchmark-flink
by echauchot
 */


package ca.uwaterloo.cs451.index;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Collection;
import java.util.Collections;

public class EndOfStreamWindow extends WindowAssigner<Object, TimeWindow> {

    public static final EndOfStreamWindow INSTANCE = new EndOfStreamWindow();

    public static final TimeWindow TIME_WINDOW_INSTANCE =
            new TimeWindow(Long.MIN_VALUE, 2);

    public EndOfStreamWindow() {}

    @Override
    public Collection<TimeWindow> assignWindows(
            Object row, long timestamp, WindowAssigner.WindowAssignerContext context) {
        return Collections.singletonList(TIME_WINDOW_INSTANCE);
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return EventTimeTrigger.create();
    }

    @Override public TypeSerializer<TimeWindow> getWindowSerializer(
            ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public String toString() {
        return "EndOfStreamWindow()";
    }

    @Override
    public boolean isEventTime() {
        return true;
    }
}
