/**
 * Copyright (c) 2011-2012, Thilo Planz. All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package v7db.files.buckets;

import java.net.UnknownHostException;
import java.util.Properties;

import v7db.files.Configuration;
import v7db.files.GridFSContentStorage;
import v7db.files.Tenants;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class BucketsServiceConfiguration {

	private final Properties properties;

	private DB db;

	public BucketsServiceConfiguration(Properties properties) {
		this.properties = properties;
		String mode = Tenants.getTenancyMode(properties);
		if (!"single".equals(mode))
			throw new IllegalArgumentException("unsupported tenancy mode: "
					+ mode);
	}

	public void init() throws UnknownHostException, MongoException {
		Mongo mongo = Configuration.getMongo();
		db = mongo.getDB(Tenants.getTenantDbName(mongo, properties, null));
	}

	public DBCollection getBucketCollection() {
		return db.getCollection("v7.buckets");
	}

	public GridFSContentStorage getContentStorage() {
		return new GridFSContentStorage(db);
	}

}
