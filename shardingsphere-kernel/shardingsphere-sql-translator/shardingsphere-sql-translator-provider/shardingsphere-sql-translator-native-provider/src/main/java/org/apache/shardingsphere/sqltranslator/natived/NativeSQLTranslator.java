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

package org.apache.shardingsphere.sqltranslator.natived;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.shardingsphere.infra.database.type.DatabaseType;
import org.apache.shardingsphere.infra.database.type.DatabaseTypeFactory;
import org.apache.shardingsphere.infra.federation.optimizer.context.parser.dialect.OptimizerSQLDialectBuilderFactory;
import org.apache.shardingsphere.infra.federation.optimizer.converter.SQLNodeConverterEngine;
import org.apache.shardingsphere.sql.parser.api.CacheOption;
import org.apache.shardingsphere.sql.parser.api.SQLParserEngine;
import org.apache.shardingsphere.sql.parser.api.SQLVisitorEngine;
import org.apache.shardingsphere.sql.parser.api.parser.SQLParser;
import org.apache.shardingsphere.sql.parser.core.ParseASTNode;
import org.apache.shardingsphere.sql.parser.core.SQLParserFactory;
import org.apache.shardingsphere.sql.parser.core.database.parser.DatabaseTypedSQLParserFacadeFactory;
import org.apache.shardingsphere.sql.parser.core.database.visitor.SQLVisitorFactory;
import org.apache.shardingsphere.sql.parser.core.database.visitor.SQLVisitorRule;
import org.apache.shardingsphere.sql.parser.spi.DatabaseTypedSQLParserFacade;
import org.apache.shardingsphere.sql.parser.sql.common.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.mysql.MySQLStatement;
import org.apache.shardingsphere.sqltranslator.spi.SQLTranslator;

import java.util.Properties;

/**
 * Native SQL translator.
 */
public final class NativeSQLTranslator implements SQLTranslator {
    
    @Override
    public String translate(final String sql, final SQLStatement statement, final DatabaseType frontendDatabaseType, final DatabaseType backendDatabaseType) {

        CacheOption cacheOption = new CacheOption(128, 1024L, 4);
        SQLParserEngine parserEngine = new SQLParserEngine(frontendDatabaseType.getType(), cacheOption);
        ParseASTNode parseASTNode = parserEngine.parse(sql, false);
        SQLVisitorEngine visitorEngine = new SQLVisitorEngine(backendDatabaseType.getType(), "STATEMENT", false, new Properties()); ;

        //FIXME support translate
        SQLStatement sqlStatement = visitorEngine.visit(parseASTNode);
        SqlNode actual = SQLNodeConverterEngine.convertToSQLNode(sqlStatement);
        return actual.toString();
    }
    
    @Override
    public String getType() {
        return "NATIVE";
    }


    private SqlParser.Config createConfig(final DatabaseType databaseType) {
        CalciteConnectionConfig connectionConfig = new CalciteConnectionConfigImpl(createSQLDialectProperties(databaseType));
        return SqlParser.config().withLex(connectionConfig.lex()).withUnquotedCasing(Casing.UNCHANGED)
                .withIdentifierMaxLength(SqlParser.DEFAULT_IDENTIFIER_MAX_LENGTH).withConformance(connectionConfig.conformance()).withParserFactory(SqlParserImpl.FACTORY);
    }

    private Properties createSQLDialectProperties(final DatabaseType databaseType) {
        Properties result = new Properties();
        result.setProperty(CalciteConnectionProperty.TIME_ZONE.camelName(), "UTC");
        result.putAll(OptimizerSQLDialectBuilderFactory.build(databaseType));
        return result;
    }

}
