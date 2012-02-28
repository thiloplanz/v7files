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
import java.io.InputStream;
import java.util.List;

import org.apache.commons.codec.binary.Hex;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

class OutOfBand {

	static BSONObject basedOnGridFSContents(byte[] sha) {
		return new BasicBSONObject("_id", sha);
	}

	/**
	 * open an InputStream for an object that uses "alt" storage
	 * 
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	static InputStream getInputStream(GridFSContentStorage storage,
			BSONObject gridFile) throws IOException {
		List<BSONObject> alt = (List<BSONObject>) gridFile.get("alt");
		if (alt == null || alt.isEmpty())
			throw new IOException(
					"missing or empty `alt` field for out-of-band storage on file "
							+ Hex.encodeHexString((byte[]) gridFile.get("_id")));
		IOException error = null;
		for (BSONObject o : alt) {
			try {
				String store = (String) o.get("store");
				if ("cat".equals(store))
					return Concatenation.getInputStream(storage, o);
				throw new IOException("unsupported storage scheme '" + store
						+ "' on file "
						+ Hex.encodeHexString((byte[]) gridFile.get("_id")));
			}

			catch (Exception e) {
				if (error == null)
					if (e instanceof IOException)
						error = (IOException) e;
					else
						error = new IOException("error on alt storage " + o, e);
				else
					error = new IOException("error on alt storage " + o + ":\n"
							+ e, error);
			}
		}
		throw error;
	}
}
