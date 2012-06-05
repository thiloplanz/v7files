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

package v7db.files.mongodb;

import java.util.ArrayList;
import java.util.List;

import org.bson.BSONObject;


import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

class Vermongo {

	static final String _VERSION = "_version";

	/**
	 * inserts a new object into the collection. The _version property must not
	 * be present in the object, and will be set to 1 (integer).
	 * 
	 * @param object
	 */
	static void insert(DBCollection collection, DBObject object) {
		if (object.containsField(_VERSION))
			throw new IllegalArgumentException();

		object.put(_VERSION, 1);
		collection.insert(object, WriteConcern.SAFE);
	}

	/**
	 * updates an existing object. The object must have been the _version
	 * property set to the version number of the base revision (i.e. the version
	 * number to be replaced). If the current version in the DB does not have a
	 * matching version number, the operation aborts with an
	 * UpdateConflictException.
	 * 
	 * After the update is successful, _version in the object is updated to the
	 * new version number.
	 * 
	 * The version that was replaced is moved into the collection's shadow
	 * collection.
	 * 
	 * @param collection
	 * @param object
	 * @throws UpdateConflictException
	 */
	static void update(DBCollection collection, DBObject object)
			throws UpdateConflictException {
		if (!object.containsField(_VERSION))
			throw new IllegalArgumentException(
					"the base version number needs to be included as _version");

		int baseVersion = (Integer) object.get(_VERSION);

		// load the base version
		{
			DBObject base = collection.findOne(new BasicDBObject("_id",
					getId(object)));
			if (base == null) {
				throw new IllegalArgumentException(
						"document to update not found in collection");
			}
			Object bV = base.get(_VERSION);
			if (bV instanceof Integer) {
				if (baseVersion != (Integer) bV) {
					throw new UpdateConflictException(object, base);
				}
			} else {
				throw new UpdateConflictException(object, base);
			}
			// copy to shadow
			DBCollection shadow = getShadowCollection(collection);
			base.put("_id", new BasicDBObject("_id", getId(base)).append(
					_VERSION, baseVersion));
			WriteResult r = shadow.insert(base, WriteConcern.SAFE);

			// TODO: if already there, no error
			r.getLastError().throwOnError();
		}

		try {
			object.put(_VERSION, baseVersion + 1);
			DBObject found = collection.findAndModify(new BasicDBObject("_id",
					getId(object)).append(_VERSION, baseVersion), object);

			if (found == null) {
				// document has changed in the mean-time. get the latest version
				// again
				DBObject base = collection.findOne(new BasicDBObject("_id",
						getId(object)));
				if (base == null) {
					throw new IllegalArgumentException(
							"document to update not found in collection");
				}
				throw new UpdateConflictException(object, base);
			}

		} catch (RuntimeException e) {
			object.put(_VERSION, baseVersion);
			throw e;
		}

	}

	/**
	 * @return the _id property of the object
	 */
	static Object getId(BSONObject o) {
		return o.get("_id");
	}

	/**
	 * @return the version number of the object (from the _version property)
	 */

	static Integer getVersion(BSONObject o) {
		return (Integer) o.get(_VERSION);
	}

	/**
	 * @return true, if the object represents a dummy version inserted to mark a
	 *         deleted version
	 */
	static boolean isDeletedDummyVersion(BSONObject o) {
		return o.get(_VERSION).toString().startsWith("deleted:");
	}

	/**
	 * deletes the object without checking for conflicts. An existing version is
	 * moved to the shadow collection, along with a dummy version to mark the
	 * deletion. This dummy version can contain optional meta-data (such as who
	 * deleted the object, and when).
	 */
	static DBObject remove(DBCollection collection, Object id,
			BSONObject metaData) {
		DBObject base = collection.findOne(new BasicDBObject("_id", id));
		if (base == null)
			return null;

		// copy to shadow
		DBCollection shadow = getShadowCollection(collection);
		int version = getVersion(base);
		BasicDBObject revId = new BasicDBObject("_id", getId(base)).append(
				_VERSION, version);
		base.put("_id", revId);
		WriteResult r = shadow.insert(base, WriteConcern.SAFE);

		// TODO: if already there, no error
		r.getLastError().throwOnError();

		// add the dummy version
		BasicDBObject dummy = new BasicDBObject("_id", revId.append(_VERSION,
				version + 1)).append(_VERSION, "deleted:" + (version + 1));
		if (metaData != null)
			dummy.putAll(metaData);
		r = shadow.insert(dummy, WriteConcern.SAFE);
		// TODO: if already there, no error
		r.getLastError().throwOnError();

		collection.remove(new BasicDBObject("_id", id));
		return base;

	}

	/**
	 * @return the shadow collection wherein the old versions of documents are
	 *         stored
	 */
	static DBCollection getShadowCollection(DBCollection c) {
		return c.getCollection("vermongo");
	}

	/**
	 * @return an old version of the document with the given id, at the given
	 *         version number
	 */

	static DBObject getOldVersion(DBCollection c, Object id, int versionNumber) {

		BasicDBObject query = new BasicDBObject("_id", new BasicDBObject("_id",
				id).append("_version", versionNumber));

		DBObject result = getShadowCollection(c).findOne(query);
		if (result == null)
			return null;
		result.put("_id", ((BasicDBObject) getId(result)).get("_id"));
		return result;
	}

	/**
	 * The list of old versions does not include the current version of the
	 * document, but it does include dummy entries to mark the deletion (if the
	 * document was deleted). The list is ordered by version number.
	 * 
	 * @return the list of old version of the document with the given id
	 */
	static List<DBObject> getOldVersions(DBCollection c, Object id) {
		DBObject query = QueryUtils.between("_id", new BasicDBObject("_id", id)
				.append("_version", 0), new BasicDBObject("_id", id).append(
				"_version", Integer.MAX_VALUE));

		List<DBObject> result = new ArrayList<DBObject>();
		for (DBObject o : getShadowCollection(c).find(query).sort(
				new BasicDBObject("_id", 1))) {
			o.put("_id", ((BasicDBObject) getId(o)).get("_id"));
			result.add(o);
		}

		return result;
	}

}
