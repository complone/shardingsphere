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

package org.apache.shardingsphere.authority.model;

import java.util.Collection;

/**
 * ShardingSphere Privileges.
 */
public interface ShardingSpherePrivileges {
    
    /**
     * Set super privilege.
     */
    void setSuperPrivilege();
    
    /**
     * Has privileges.
     *
     * @param database schema
     * @return has privileges or not
     */
    boolean hasPrivileges(String database);
    
    /**
     * Has privileges.
     *
     * @param privileges privileges
     * @return has privileges or not
     */
    boolean hasPrivileges(Collection<PrivilegeType> privileges);
    
    /**
     * Has privileges.
     *
     * @param accessSubject access subject
     * @param privileges privileges
     * @return has privileges or not
     */
    boolean hasPrivileges(AccessSubject accessSubject, Collection<PrivilegeType> privileges);
}
