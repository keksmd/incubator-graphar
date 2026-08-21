/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.graphar.delta;

/**
 * A journal that records nothing.
 *
 * <p>A delta whose contents can be replayed from the ledger it was derived from does not need a
 * second durable copy of them. Making that case explicit keeps the delta free of a nullable journal
 * and keeps the decision visible where the delta is built.
 */
final class NoDeltaJournal implements DeltaJournal {
    @Override
    public long edgeFloor() {
        return 0L;
    }

    @Override
    public void vertex(int typeOrdinal, String externalId) {}

    @Override
    public void edge(int source, int target) {}

    @Override
    public void sync() {}

    @Override
    public void replay(Visitor visitor) {}

    @Override
    public void rewrite(long baseVertexCount, long edgeFloor, Content content) {}

    @Override
    public void close() {}
}
