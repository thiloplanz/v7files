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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.input.CountingInputStream;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFSDBFile;

/**
 * "Out-of-band" storage scheme "cat" (concatenating one or more pieces of
 * existing GridFS content).
 * 
 * <pre>
 * {   store : 'cat',
 *     base: [
 *             BinData(...) ,   // (short) inlined bytes
 *             { _id : BinData(...), len: 1234 } // existing content,
 *                    //  len can be shorter than the whole file (partial concat)
 *                    //  but is always given (to make it possible to skip chunks)
 *             { _id: BinData(...), len: 1234, off: 12 }
 *                    // also possible to specify an offset
 *             { _id: BinData(...), len: 1234, store: 'gzip', off: 123, end: 999 }
 *                    // treat the segment segment specified by off/end 
 *                    // as gzip data, "len" is the result length
 * 
 * </pre>
 * 
 */

class Concatenation {

	private static final Logger log = LoggerFactory
			.getLogger(Concatenation.class);

	/**
	 * Store the concatenated contents of the given pieces
	 * 
	 * @param pieces
	 *            the parts to be concatenated, can be BSONObject (for other
	 *            GridFS contents) or byte[] (for short literal data)
	 * @return the SHA1 for the concatenation
	 * @throws IOException
	 */
	static byte[] storeConcatenation(V7GridFS gridFS, String filename,
			Object fileId, String contentType, Object... pieces)
			throws IOException {
		DBObject cat = calculateConcatenation(gridFS, pieces);
		byte[] sha = (byte[]) cat.get("_id");
		cat.removeField("_id");
		gridFS.registerAlt(sha, cat, filename, fileId, contentType);

		return sha;
	}

	private static BSONObject findNestedConcat(BSONObject base) {
		List<?> alts = (List<?>) base.get("alt");
		if (alts == null)
			return null;
		for (Object alt : alts) {
			try {
				BSONObject b = (BSONObject) alt;
				if ("cat".equals(b.get("store"))) {
					return b;
				}
			} catch (Exception e) {
				log.warn("invalid out-of-band storage definition " + alt, e);
			}
		}
		return null;
	}

	static DBObject calculateConcatenation(V7GridFS gridFS, Object... pieces)
			throws IOException {
		BasicDBObject result = new BasicDBObject();
		Vector<CountingInputStream> ins = new Vector<CountingInputStream>(
				pieces.length);
		try {
			List<Object> bases = new ArrayList<Object>(pieces.length);
			for (Object piece : pieces) {
				// raw data
				if (piece instanceof byte[]) {
					bases.add(piece);
					ins.add(new CountingInputStream(new ByteArrayInputStream(
							(byte[]) piece)));
					continue;
				}
				if (piece instanceof BSONObject) {
					BSONObject b = (BSONObject) piece;
					Object id = b.get("_id");
					if (id instanceof byte[]) {
						if (b.keySet().size() > 1)
							throw new IllegalArgumentException(
									"invalid base object for concatenation (unsupported fields): "
											+ piece);
						GridFSDBFile f = gridFS.findContent((byte[]) id);
						if (f == null)
							throw new IOException(
									"failed to find base content for concatenation in GridFS: "
											+ Hex.encodeHexString((byte[]) id));

						BSONObject nestedConcat = findNestedConcat(f);
						if (nestedConcat != null) {
							List<?> nestedBases = (List<?>) nestedConcat
									.get("base");
							for (Object n : nestedBases) {
								bases.add(n);
								ins.add(new CountingInputStream(
										getPieceInputStream(gridFS, n)));
							}
						} else {
							bases.add(new BasicBSONObject("_id", id));
							ins.add(new CountingInputStream(gridFS
									.readContent(f)));
						}
						continue;
					}
					throw new IllegalArgumentException(
							"invalid base object for concatenation (missing or invalid _id): "
									+ piece);
				}
				throw new IllegalArgumentException(
						"invalid base object for concatenation (unsupported type): "
								+ piece);
			}
			SequenceInputStream in = new SequenceInputStream(ins.elements());
			byte[] sha = DigestUtils.sha(in);
			in.close();
			result.put("_id", sha);
			for (int i = 0; i < bases.size(); i++) {
				Object base = bases.get(i);
				if (base instanceof BSONObject) {
					((BSONObject) base).put("len", ins.get(i).getByteCount());
				}
			}
			result.put("base", bases);
			result.put("store", "cat");
		} finally {
			for (InputStream in : ins) {
				in.close();
			}
		}
		return result;
	}

	private static InputStream getPieceInputStream(V7GridFS gridFS, Object piece)
			throws IOException {
		// raw data
		if (piece instanceof byte[])
			return new ByteArrayInputStream((byte[]) piece);

		if (piece instanceof BSONObject) {
			BSONObject b = (BSONObject) piece;
			Object id = b.get("_id");
			if (id instanceof byte[]) {
				if (b.keySet().size() > 2 || !b.containsField("len"))
					throw new IllegalArgumentException(
							"invalid base object for concatenation (unsupported fields): "
									+ piece);
				InputStream f = gridFS.readContent((byte[]) id);
				if (f == null)
					throw new IOException(
							"failed to find base content for concatenation in GridFS: "
									+ Hex.encodeHexString((byte[]) id));
				return f;
			}
			throw new IllegalArgumentException(
					"invalid base object for concatenation (missing or invalid _id): "
							+ piece);
		}
		throw new IllegalArgumentException(
				"invalid base object for concatenation (unsupported type): "
						+ piece);
	}

	static InputStream getInputStream(V7GridFS gridFS, BSONObject cat)
			throws IOException {
		String store = (String) cat.get("store");
		if (!"cat".equals(store))
			throw new IllegalArgumentException("cannot handle non-cat " + store);
		List<?> bases = (List<?>) cat.get("base");
		if (bases == null || bases.isEmpty())
			throw new IllegalArgumentException(
					"missing `base` field for concatenation");
		Vector<InputStream> ins = new Vector<InputStream>(bases.size());
		try {
			for (Object piece : bases) {
				ins.add(getPieceInputStream(gridFS, piece));
			}
		} finally {
			if (ins.size() != bases.size())
				for (InputStream in : ins) {
					in.close();
				}
		}
		return new SequenceInputStream(ins.elements());
	}
}
