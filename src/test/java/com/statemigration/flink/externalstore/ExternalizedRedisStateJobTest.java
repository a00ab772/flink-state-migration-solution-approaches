package com.statemigration.flink.externalstore;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.junit.jupiter.api.Test;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.*;

public class ExternalizedRedisStateJobTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testAsyncInvokeWithMockedRedis() throws Exception {
        // Arrange
        ExternalizedRedisStateJob.ExternalizedRedisStateFunction function =
                new ExternalizedRedisStateJob.ExternalizedRedisStateFunction();

        RedisAsyncCommands<String, String> mockCommands = mock(RedisAsyncCommands.class);
        RedisFuture<String> mockRedisFuture = mock(RedisFuture.class);

        CompletableFuture<String> completedFuture = CompletableFuture.completedFuture("{\"tier\":\"gold\"}");
        when(mockRedisFuture.toCompletableFuture()).thenReturn(completedFuture);
        when(mockCommands.get("user_1")).thenReturn(mockRedisFuture);

        java.lang.reflect.Field field = ExternalizedRedisStateJob.ExternalizedRedisStateFunction.class.getDeclaredField("asyncCommands");
        field.setAccessible(true);
        field.set(function, mockCommands);

        ResultFuture<String> mockResultFuture = mock(ResultFuture.class);
        ExternalizedRedisStateJob.StreamEvent event = new ExternalizedRedisStateJob.StreamEvent("user_1", "click");

        // Act
        function.asyncInvoke(event, mockResultFuture);

        // Assert
        verify(mockResultFuture, times(1)).complete(
                Collections.singleton("Processed user user_1 with external state: {\"tier\":\"gold\"}")
        );
    }
}