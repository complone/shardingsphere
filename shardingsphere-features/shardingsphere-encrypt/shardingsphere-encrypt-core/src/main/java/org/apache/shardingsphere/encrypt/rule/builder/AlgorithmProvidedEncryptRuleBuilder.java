/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.encrypt.rule.builder;

import org.apache.shardingsphere.encrypt.algorithm.config.AlgorithmProvidedEncryptRuleConfiguration;
import org.apache.shardingsphere.encrypt.constant.EncryptOrder;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.infra.rule.builder.schema.DatabaseRuleBuilder;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.Map;

/**
 * Algorithm provided encrypt rule builder.
 */
public final class AlgorithmProvidedEncryptRuleBuilder implements DatabaseRuleBuilder<AlgorithmProvidedEncryptRuleConfiguration> {
    
    @Override
    public EncryptRule build(final AlgorithmProvidedEncryptRuleConfiguration config, final String databaseName,
                             final Map<String, DataSource> dataSources, final Collection<ShardingSphereRule> builtRules, final ConfigurationProperties props) {
        return new EncryptRule(config);
    }
    
    @Override
    public int getOrder() {
        return EncryptOrder.ALGORITHM_PROVIDER_ORDER;
    }
    
    @Override
    public Class<AlgorithmProvidedEncryptRuleConfiguration> getTypeClass() {
        return AlgorithmProvidedEncryptRuleConfiguration.class;
    }
}
