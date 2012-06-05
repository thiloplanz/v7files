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
package v7db.files;

import static v7db.files.mongodb.BSONUtils.notNull;
import static v7db.files.mongodb.BSONUtils.toLong;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.BSONObject;

import v7db.files.mongodb.BSONUtils;

/**
 * Just like BSONUtils, but for Maps.
 */

public class MapUtils {

	private static Object get(Map<?, ?> b, String fieldName) {
		if (!fieldName.contains("."))
			return notNull(b.get(fieldName));
		String[] path = StringUtils.split(fieldName, ".", 2);
		Object nested = b.get(path[0]);
		if (nested == null)
			return null;
		if (nested instanceof BSONObject)
			return BSONUtils.get((BSONObject) nested, path[1]);
		if (nested instanceof Map<?, ?>)
			return get((Map<?, ?>) nested, path[1]);
		throw new IllegalArgumentException("cannot get field `" + fieldName
				+ "` of " + b);
	}

	private static Object getRequired(Map<?, ?> b, String fieldName) {
		Object x = get(b, fieldName);
		if (x == null)
			throw new IllegalArgumentException("required field `" + fieldName
					+ "` is missing in " + b);
		return x;
	}

	public static long getRequiredLong(Map<?, ?> b, String fieldName) {
		return toLong(getRequired(b, fieldName)).longValue();
	}

	public static Long getLong(Map<?, ?> b, String fieldName) {
		return toLong(get(b, fieldName));
	}

	public static String getString(Map<?, ?> b, String fieldName) {
		return BSONUtils.toString(get(b, fieldName));
	}

	public static byte[] getRequiredBytes(Map<?, ?> b, String fieldName) {
		Object x = getRequired(b, fieldName);
		if (x == null)
			return null;
		if (x instanceof byte[])
			return ((byte[]) x).clone();
		throw new IllegalArgumentException("cannot convert `" + x
				+ "` into a byte[]");
	}

	public static Object[] values(Map<?, ?> b, String fieldName) {
		Object x = get(b, fieldName);
		if (x == null)
			return ArrayUtils.EMPTY_OBJECT_ARRAY;
		if (x instanceof List<?>)
			return ((List<?>) x).toArray();
		if (x instanceof Object[])
			return ArrayUtils.clone((Object[]) x);
		return new Object[] { x };
	}

	public static void supportedFields(Map<String, ?> o, String... fields) {
		f: for (String f : o.keySet()) {
			for (String check : fields) {
				if (check.equals(f))
					continue f;
			}
			throw new UnsupportedOperationException("only "
					+ Arrays.toString(fields) + " are supported: " + o);
		}
	}

	public static void supportedAndRequiredFields(Map<String, ?> o,
			String... fields) {
		for (String f : fields) {
			if (!o.containsKey(f))
				throw new UnsupportedOperationException(
						"missing required field '" + f + "': " + o);
		}
		supportedFields(o, fields);
	}

	@SuppressWarnings("unchecked")
	public static Map<String, Object> supportJustStringKeys(Map<?, ?> o) {
		for (Object x : o.keySet()) {
			if (!(x instanceof String))
				throw new UnsupportedOperationException(
						"only string keys are supported, not " + x);
		}
		return (Map<String, Object>) o;
	}
}
