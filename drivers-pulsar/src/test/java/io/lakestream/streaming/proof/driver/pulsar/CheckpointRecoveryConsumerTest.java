/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package io.lakestream.streaming.proof.driver.pulsar;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import java.io.UncheckedIOException;
import org.apache.pulsar.client.api.v5.Checkpoint;
import org.apache.pulsar.client.api.v5.CheckpointConsumer;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class CheckpointRecoveryConsumerTest {
    @Test
    public void restoresSerializedPositionAfterProcessingThreshold() throws Exception {
        CheckpointRecoveryConsumer.Factory factory = mock(CheckpointRecoveryConsumer.Factory.class);
        CheckpointConsumer<byte[]> first = mock(CheckpointConsumer.class);
        CheckpointConsumer<byte[]> second = mock(CheckpointConsumer.class);
        when(factory.create(any())).thenReturn(first, second);
        when(first.checkpoint()).thenReturn(Checkpoint.latest());
        try (CheckpointRecoveryConsumer recovery = new CheckpointRecoveryConsumer(factory, 2)) {
            recovery.messageProcessed();
            verify(first, never()).checkpoint();
            recovery.messageProcessed();
            recovery.receive();
            verify(first).close();
            verify(second).receive();
            ArgumentCaptor<Checkpoint> positions = ArgumentCaptor.forClass(Checkpoint.class);
            verify(factory, times(2)).create(positions.capture());
            assertEquals(positions.getAllValues().get(0).toByteArray(), Checkpoint.earliest().toByteArray());
            assertEquals(positions.getAllValues().get(1).toByteArray(), Checkpoint.latest().toByteArray());
        }
        verify(second).close();
    }

    @Test
    public void failedRestoreRetriesSavedPositionWithoutFallingBackToEarliest() throws Exception {
        CheckpointRecoveryConsumer.Factory factory = mock(CheckpointRecoveryConsumer.Factory.class);
        CheckpointConsumer<byte[]> first = mock(CheckpointConsumer.class);
        CheckpointConsumer<byte[]> second = mock(CheckpointConsumer.class);
        when(factory.create(any())).thenReturn(first)
                .thenThrow(new PulsarClientException("temporary restore failure")).thenReturn(second);
        when(first.checkpoint()).thenReturn(Checkpoint.latest());
        try (CheckpointRecoveryConsumer recovery = new CheckpointRecoveryConsumer(factory, 1)) {
            recovery.messageProcessed();
            expectThrows(UncheckedIOException.class, recovery::receive);
            recovery.receive();
            ArgumentCaptor<Checkpoint> positions = ArgumentCaptor.forClass(Checkpoint.class);
            verify(factory, times(3)).create(positions.capture());
            assertEquals(positions.getAllValues().get(1).toByteArray(), Checkpoint.latest().toByteArray());
            assertEquals(positions.getAllValues().get(2).toByteArray(), Checkpoint.latest().toByteArray());
            verify(first, times(1)).close();
        }
    }

    @Test
    public void invalidCheckpointDoesNotCloseTheLiveConsumer() throws Exception {
        CheckpointRecoveryConsumer.Factory factory = mock(CheckpointRecoveryConsumer.Factory.class);
        CheckpointConsumer<byte[]> first = mock(CheckpointConsumer.class);
        Checkpoint invalid = mock(Checkpoint.class);
        when(invalid.toByteArray()).thenReturn(new byte[] {(byte) 255});
        when(first.checkpoint()).thenReturn(invalid);
        when(factory.create(any())).thenReturn(first);
        try (CheckpointRecoveryConsumer recovery = new CheckpointRecoveryConsumer(factory, 1)) {
            recovery.messageProcessed();
            expectThrows(UncheckedIOException.class, recovery::receive);
            expectThrows(UncheckedIOException.class, recovery::receive);
            verify(first, never()).close();
            verify(first, never()).receive();
        }
    }

    @Test
    public void failedCloseIsRetriedBeforeCreatingAReplacement() throws Exception {
        CheckpointRecoveryConsumer.Factory factory = mock(CheckpointRecoveryConsumer.Factory.class);
        CheckpointConsumer<byte[]> first = mock(CheckpointConsumer.class);
        CheckpointConsumer<byte[]> second = mock(CheckpointConsumer.class);
        when(factory.create(any())).thenReturn(first, second);
        when(first.checkpoint()).thenReturn(Checkpoint.latest());
        doThrow(new PulsarClientException("temporary close failure"))
                .doNothing().when(first).close();
        try (CheckpointRecoveryConsumer recovery = new CheckpointRecoveryConsumer(factory, 1)) {
            recovery.messageProcessed();
            expectThrows(PulsarClientException.class, recovery::receive);
            verify(factory, times(1)).create(any());
            recovery.receive();
            verify(first, times(2)).close();
            verify(second).receive();
        }
    }

    @Test
    public void closeDuringPendingRestartClosesOldConsumerAndDoesNotRecreateIt() throws Exception {
        CheckpointRecoveryConsumer.Factory factory = mock(CheckpointRecoveryConsumer.Factory.class);
        CheckpointConsumer<byte[]> first = mock(CheckpointConsumer.class);
        when(factory.create(any())).thenReturn(first);
        when(first.checkpoint()).thenReturn(Checkpoint.latest());
        CheckpointRecoveryConsumer recovery = new CheckpointRecoveryConsumer(factory, 1);
        recovery.messageProcessed();
        recovery.close();
        recovery.close();
        expectThrows(IllegalStateException.class, recovery::receive);
        verify(first, times(1)).close();
        verify(factory, times(1)).create(any());
    }
}
