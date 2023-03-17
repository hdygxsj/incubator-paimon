/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.table.sink;

import org.apache.paimon.file.io.CompactIncrement;
import org.apache.paimon.file.io.NewFilesIncrement;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.paimon.file.manifest.ManifestCommittableSerializerTest.randomCompactIncrement;
import static org.apache.paimon.file.manifest.ManifestCommittableSerializerTest.randomNewFilesIncrement;
import static org.apache.paimon.file.mergetree.compact.MergeTreeCompactManagerTest.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CommitMessageSerializer}. */
public class CommitMessageSerializerTest {

    @Test
    public void test() throws IOException {
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        NewFilesIncrement newFilesIncrement = randomNewFilesIncrement();
        CompactIncrement compactIncrement = randomCompactIncrement();
        CommitMessage committable =
                new CommitMessageImpl(row(0), 1, newFilesIncrement, compactIncrement);
        CommitMessage newCommittable = serializer.deserialize(2, serializer.serialize(committable));
        assertThat(newCommittable).isEqualTo(committable);
    }
}