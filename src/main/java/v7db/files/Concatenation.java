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

import static v7db.files.BSONUtils.getInteger;
import static v7db.files.BSONUtils.getRequiredInt;
import static v7db.files.BSONUtils.putIntegerOrLong;
import static v7db.files.BSONUtils.removeField;
import static v7db.files.BSONUtils.toInteger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.lang3.ArrayUtils;
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
 *             { _id: BinData(...), len: 1234, store: 'zip', off: 123, end: 999 }
 *                    // treat the segment segment specified by off/end 
 *                    // as a zip file entry, "len" is the uncompressed data length
 * 
 * </pre>
 * 
 */

class Concatenation {

	private static final Logger log = LoggerFactory
			.getLogger(Concatenation.class);

	/**
	 * concatenate existing GridFS content
	 * 
	 * @return an object for use in
	 *         {@link #storeConcatenation(V7GridFS, String, Object, String, Object...)}
	 */
	static Object gridFSContents(byte[] sha) {
		return OutOfBand.basedOnGridFSContents(sha);
	}

	/**
	 * concatenate a piece from existing GridFS content
	 * 
	 * @return an object for use in
	 *         {@link #storeConcatenation(V7GridFS, String, Object, String, Object...)}
	 */
	static Object gridFSContents(byte[] sha, int off, int len) {
		BSONObject x = OutOfBand.basedOnGridFSContents(sha);
		x.put("len", len);
		x.put("off", off);
		return x;
	}

	/**
	 * concatenate a piece from existing GridFS content, treating it as a zip
	 * file entry (including the header)
	 * 
	 * @return an object for use in
	 *         {@link #storeConcatenation(V7GridFS, String, Object, String, Object...)}
	 */
	static Object zipEntryInGridFSContents(byte[] sha, int off, int lenHeader,
			int lenData) {
		BSONObject x = OutOfBand.basedOnGridFSContents(sha);
		x.put("end", off + lenHeader + lenData);
		x.put("off", off);
		x.put("store", "zip");
		return x;
	}

	/**
	 * concatenate raw (literal, inline) bytes
	 */
	static Object bytes(byte[] data) {
		return data;
	}

	/**
	 * concatenate raw (literal, inline) bytes
	 */
	static Object bytes(byte[] data, int off, int len) {
		return ArrayUtils.subarray(data, off, off + len);
	}

	/**
	 * Store the concatenated contents of the given pieces
	 * 
	 * @param pieces
	 *            the parts to be concatenated, can be BSONObject (for other
	 *            GridFS contents) or byte[] (for short literal data)
	 * @return the SHA1 for the concatenation
	 * @throws IOException
	 */
	static byte[] storeConcatenation(GridFSContentStorage storage,
			String filename, Object fileId, String contentType,
			Object... pieces) throws IOException {
		DBObject cat = calculateConcatenation(storage, pieces);
		// edge-case: became inline data only
		if (cat.containsField("in")) {
			return GridFSContentStorage.getSha(storage
					.insertContentsAndBackRefs((byte[]) cat.get("in"), fileId,
							0, filename, contentType));
		}

		byte[] sha = (byte[]) cat.get("_id");
		cat.removeField("_id");
		storage.registerAlt(sha, cat, filename, fileId, contentType);

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

	private static void addNestedPiece(GridFSContentStorage storage,
			Object piece, List<Object> bases, List<CountingInputStream> ins)
			throws IOException {
		if (piece instanceof BSONObject) {
			// the reference to a base takes at least 30 bytes
			// so for shorter segments, inline the data directly
			// "_id" 3 + SHA 20 + "len" 3 + int32 4
			int minLength = 30;
			if (((BSONObject) piece).containsField("off"))
				minLength += 7;
			if (getPieceLength(piece) < minLength) {
				addBytes(IOUtils
						.toByteArray(getPieceInputStream(storage, piece)),
						bases, ins);
				return;
			}
		}

		bases.add(piece);
		ins.add(new CountingInputStream(getPieceInputStream(storage, piece)));
	}

	private static void addNestedConcats(GridFSContentStorage storage,
			BSONObject nestedConcat, List<Object> bases,
			List<CountingInputStream> ins, Integer off, Integer len)
			throws IOException {
		List<?> nestedBases = (List<?>) nestedConcat.get("base");

		int skip = 0;
		if (off != null)
			skip = off.intValue();
		Integer left = null;
		if (len != null)
			left = len.intValue();

		for (Object n : nestedBases) {
			int plen = getPieceLength(n);

			if (skip > 0) {
				if (plen <= skip) {
					// skip the whole thing
					skip -= plen;
					continue;
				}
				// skip a part
				plen -= skip;
				n = addOffsetToPiece(n, skip);
				skip = 0;
			}
			if (left != null) {
				if (plen < left) {
					// include the whole rest
					addNestedPiece(storage, n, bases, ins);
					left -= plen;
					continue;
				}
				if (plen > left) {
					// include only a part
					n = trimPieceToLength(n, left);
				}
				addNestedPiece(storage, n, bases, ins);
				// no more pieces after this
				return;
			}

		}
		return;
	}

	private static void addBytes(byte[] piece, List<Object> bases,
			List<CountingInputStream> ins) {
		// consecutive byte arrays are concatenated directly
		if (!bases.isEmpty()) {
			Object prev = bases.get(bases.size() - 1);
			if (prev instanceof byte[]) {
				bases.set(bases.size() - 1, ArrayUtils.addAll((byte[]) prev,
						piece));
				return;
			}
		}
		bases.add(piece);
		ins.add(new CountingInputStream(new ByteArrayInputStream(piece)));
	}

	private static String getStore(BSONObject b) {
		String store = BSONUtils.toString(removeField(b, "store"));
		if ("raw".equals(store))
			store = null;

		if (store != null && !"zip".equals(store)) {
			throw new IllegalArgumentException(
					"invalid base object for concatenation (unsupported store type): "
							+ store);
		}
		return store;
	}

	static DBObject calculateConcatenation(GridFSContentStorage storage,
			Object... pieces) throws IOException {
		Vector<CountingInputStream> ins = new Vector<CountingInputStream>(
				pieces.length);
		try {
			List<Object> bases = new ArrayList<Object>(pieces.length);
			for (Object piece : pieces) {
				// raw data
				if (piece instanceof byte[]) {
					addBytes((byte[]) piece, bases, ins);
					continue;
				}
				if (piece instanceof BSONObject) {
					BSONObject b = (BSONObject) piece;
					Object _id = b.get("_id");
					if (_id instanceof byte[]) {
						byte[] id = (byte[]) _id;
						Integer offset = toInteger(removeField(b, "off"));
						Integer length = toInteger(removeField(b, "len"));
						Integer end = null;

						String store = getStore(b);

						if (store != null) {
							end = toInteger(removeField(b, "end"));
							if (end == null)
								throw new IllegalArgumentException(
										"invalid base object for concatenation (missing 'end' parameter for store type "
												+ store + ")");
						}

						if (b.keySet().size() > 1)
							throw new IllegalArgumentException(
									"invalid base object for concatenation (unsupported fields): "
											+ piece);

						GridFSDBFile f = storage.findContent(id);
						if (f == null)
							throw new IOException(
									"failed to find base content for concatenation in GridFS: "
											+ Hex.encodeHexString(id));

						// the reference to a base takes at least 30 bytes
						// so for shorter segments, inline the data directly
						// "_id" 3 + SHA 20 + "len" 3 + int32 4
						if (length != null) {
							int minLength = 30;
							if (offset != null)
								minLength += 7;
							if (length.intValue() < minLength) {
								addBytes(IOUtils.toByteArray(storage
										.readContent(f, offset, length)),
										bases, ins);
								continue;
							}
						}

						BSONObject nestedConcat = findNestedConcat(f);
						if (nestedConcat != null) {
							addNestedConcats(storage, nestedConcat, bases, ins,
									offset, length);
						} else {
							BSONObject x = new BasicBSONObject("_id", id);
							// "len" will be calculated automatically
							putIntegerOrLong(x, "off", offset);
							BSONUtils.putString(x, "store", store);
							if ("zip".equals(store)) {
								putIntegerOrLong(x, "end", end);
								ins.add(new CountingInputStream(Compression
										.unzip(storage.readContent(f, offset,
												length))));
							} else {
								ins.add(new CountingInputStream(storage
										.readContent(f, offset, length)));
							}
							bases.add(x);

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

			// if this ended up as just one byte array,
			// don't store it as a concatenation
			if (bases.size() == 1) {
				Object b = bases.get(0);
				if (b instanceof byte[]) {
					return new BasicDBObject("in", b);

				}
			}

			SequenceInputStream in = new SequenceInputStream(ins.elements());
			byte[] sha = DigestUtils.sha(in);
			in.close();
			BasicDBObject result = new BasicDBObject();
			result.put("_id", sha);
			for (int i = 0; i < bases.size(); i++) {
				Object base = bases.get(i);
				if (base instanceof BSONObject) {
					putIntegerOrLong((BSONObject) base, "len", ins.get(i)
							.getByteCount());
				}
			}
			result.put("base", bases);
			result.put("store", "cat");
			return result;
		} finally {
			for (InputStream in : ins) {
				in.close();
			}
		}

	}

	private static Object trimPieceToLength(Object piece, int length) {
		// raw data
		if (piece instanceof byte[]) {
			byte[] p = (byte[]) piece;
			return ArrayUtils.subarray(p, 0, length);
		}

		if (piece instanceof BSONObject) {
			BSONObject b = (BSONObject) piece;
			b.put("len", length);
			return b;
		}
		throw new IllegalArgumentException(
				"invalid base object for concatenation (unsupported type): "
						+ piece);
	}

	private static Object addOffsetToPiece(Object piece, int offset) {
		// raw data
		if (piece instanceof byte[]) {
			byte[] p = (byte[]) piece;
			return ArrayUtils.subarray(p, offset, p.length);
		}

		if (piece instanceof BSONObject) {
			BSONObject b = (BSONObject) piece;
			Integer poffset = getInteger(b, "off");
			if (poffset != null)
				b.put("off", poffset + offset);
			else
				b.put("off", offset);
			b.put("len", getRequiredInt(b, "len") - offset);
			return b;
		}
		throw new IllegalArgumentException(
				"invalid base object for concatenation (unsupported type): "
						+ piece);
	}

	private static int getPieceLength(Object piece) {
		// raw data
		if (piece instanceof byte[])
			return ((byte[]) piece).length;

		if (piece instanceof BSONObject) {
			BSONObject b = (BSONObject) piece;
			return getRequiredInt(b, "len");
		}
		throw new IllegalArgumentException(
				"invalid base object for concatenation (unsupported type): "
						+ piece);
	}

	private static InputStream getCompressedPieceInputStream(
			GridFSContentStorage storage, BSONObject piece, byte[] id,
			String store, Integer offset) throws IOException {
		int end = getRequiredInt(piece, "end");
		if (piece.keySet().size() > 3)
			throw new IllegalArgumentException(
					"invalid base object for concatenation (unsupported fields): "
							+ piece);

		int off = 0;
		if (offset != null)
			off = offset;
		int len = end - off;
		InputStream f = storage.readContent(id, off, len);
		if (f == null)
			throw new IOException(
					"failed to find base content for concatenation in GridFS: "
							+ Hex.encodeHexString(id));
		return Compression.unzip(f);

	}

	private static InputStream getPieceInputStream(
			GridFSContentStorage storage, Object piece) throws IOException {
		// raw data
		if (piece instanceof byte[])
			return new ByteArrayInputStream((byte[]) piece);

		if (piece instanceof BSONObject) {
			BSONObject b = (BSONObject) piece;
			Object id = b.get("_id");
			if (id instanceof byte[]) {
				Integer offset = toInteger(removeField(b, "off"));
				String store = getStore(b);
				if (store != null) {
					return getCompressedPieceInputStream(storage, b,
							(byte[]) id, store, offset);
				}

				if (b.keySet().size() > 2)
					throw new IllegalArgumentException(
							"invalid base object for concatenation (unsupported fields): "
									+ piece);
				int len = getPieceLength(piece);
				int off = 0;
				if (offset != null)
					off = offset;
				InputStream f = storage.readContent((byte[]) id, off, len);
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

	static InputStream getInputStream(GridFSContentStorage storage,
			BSONObject cat) throws IOException {
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
				ins.add(getPieceInputStream(storage, piece));
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
