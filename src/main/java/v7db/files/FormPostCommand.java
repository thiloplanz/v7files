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

package v7db.files;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.bson.BSONObject;
import org.bson.types.ObjectId;

import v7db.files.formpost.FormPostConfiguration;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;

class FormPostCommand {

	private static FormPostConfiguration getFormPostConfiguration(
			String endpoint) throws UnknownHostException, MongoException {
		Set<String> endpoints = Configuration.checkEndpoints();
		if (endpoints.contains(endpoint)) {
			FormPostConfiguration c = new FormPostConfiguration(Configuration
					.getEndpointProperties(endpoint));
			c.init();
			return c;
		}
		throw new IllegalArgumentException("undefined endpoint " + endpoint
				+ ", valid choices are " + StringUtils.join(endpoints, ","));
	}

	private static void create(String[] args) throws MongoException,
			IOException {
		FormPostConfiguration conf = getFormPostConfiguration(args[2]);
		DBCollection cc = conf.getControlCollection();
		BasicDBObject doc = new BasicDBObject("_id", args[3]);
		WriteResult r = cc.insert(doc);
		if (r.getError() != null)
			throw new IOException(r.getError());
	}

	private static void ls(String[] args) throws UnknownHostException,
			MongoException {
		FormPostConfiguration conf = getFormPostConfiguration(args[2]);
		DBCollection cc = conf.getControlCollection();
		BSONObject controlDoc = cc.findOne(args[3]);
		if (controlDoc == null) {
			System.err.println("FormPost control document `" + args[3]
					+ "` not found.");
			return;
		}
		Object[] uploads = BSONUtils.values(controlDoc, "data");
		controlDoc.removeField("data");
		System.out.println(controlDoc);
		for (Object upload : uploads) {
			BSONObject u = (BSONObject) upload;
			Object id = u.get("_id");
			System.out.println("   upload " + id);
			if (id instanceof ObjectId) {
				System.out.println("     "
						+ new Date(((ObjectId) id).getTime()));
			}
			Object params = u.get("parameters");
			if (params != null)
				System.out.println("     " + params);
			for (Object file : BSONUtils.values(u, "files")) {
				BSONObject f = (BSONObject) file;
				System.out.format("      %10d %80s %10s\n",
						GridFSContentStorage.getLength(f), GridFSContentStorage
								.getFilename(f), GridFSContentStorage
								.getDigest(f).substring(0, 10));
			}
			System.out.println("   ----");
		}
	}

	public static void main(String[] args) throws MongoException, IOException {

		if (args.length != 4) {
			System.err
					.println("Manage the control documents for the FormPost server:");
			System.err.println("  formPost create <endpoint> <id>");
			System.err.println("  formPost ls <endpoint> <id>");
			System.exit(1);
		}

		String command = args[1];
		if ("create".equals(command)) {
			create(args);
			return;
		}

		if ("ls".equals(command)) {
			ls(args);
			return;
		}

		System.err.println("Unsupported command '" + command + "'.");
		System.err.println("Valid commands are 'create' and 'ls'");
		System.exit(1);

	}
}
