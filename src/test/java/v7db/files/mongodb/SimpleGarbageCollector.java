/**
 * Copyright (c) 2012, Thilo Planz. All rights reserved.
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
package v7db.files.mongodb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import v7db.files.BSONUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

class SimpleGarbageCollector {

	static void purge(DBCollection contents, DBCollection references)
			throws MongoException, DecoderException {

		List<Object> refsToPurge = new ArrayList<Object>();
		Set<String> potentialGarbage = new HashSet<String>();
		Set<String> notGarbage = new HashSet<String>();

		for (DBObject x : references.find()) {
			if (x.containsField("purge")) {
				refsToPurge.add(x.get("_id"));
				for (Object r : BSONUtils.values(x, "refs")) {
					String h = Hex.encodeHexString((byte[]) r);
					potentialGarbage.add(h);
				}
				for (Object r : BSONUtils.values(x, "refHistory")) {
					String h = Hex.encodeHexString((byte[]) r);
					potentialGarbage.add(h);
				}
			} else {
				for (Object r : BSONUtils.values(x, "refs")) {
					String h = Hex.encodeHexString((byte[]) r);
					notGarbage.add(h);
				}
				for (Object r : BSONUtils.values(x, "refHistory")) {
					String h = Hex.encodeHexString((byte[]) r);
					notGarbage.add(h);
				}
			}
		}

		potentialGarbage.removeAll(notGarbage);
		// TODO: bases must not be removed
		for (String g : potentialGarbage) {
			contents.remove(new BasicDBObject("_id", Hex.decodeHex(g
					.toCharArray())), WriteConcern.SAFE);
		}
		for (Object x : refsToPurge) {
			references.remove(new BasicDBObject("_id", x), WriteConcern.SAFE);
		}
	}

}
