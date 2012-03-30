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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import v7db.files.aws.GridFSContentStorageWithS3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSInputFile;

class S3Command {

	/**
	 * @param args
	 * @throws MongoException
	 * @throws IOException
	 */
	public static void main(String[] args) throws MongoException, IOException {
		if (args.length != 3) {
			System.err.println("Migrate contents from/to S3:");
			System.err
					.println("  s3 migrate -s3        copy all stored (non-inline) contents to S3 ");
			System.err
					.println("  s3 migrate -mongo    copy all stored contents from S3 to mongodb");

			System.err.println("List contents in S3:");
			System.err
					.println("  s3 ls <shaHex>       query S3 for object info for contents with the sha prefix");
			System.exit(1);
		}

		Properties props = Configuration.getProperties();

		if ("ls".equals(args[1])) {
			String sha = args[2].toLowerCase();
			AmazonS3 s3 = GridFSContentStorageWithS3.configureS3(props);
			ObjectListing ls = s3.listObjects(props.getProperty("s3.bucket"),
					sha);
			for (S3ObjectSummary s : ls.getObjectSummaries()) {
				ObjectMetadata meta = s3.getObjectMetadata(s.getBucketName(), s
						.getKey());
				System.out.format("-      %10d %80s %10s %10s\n", s.getSize(),
						meta.getContentType(), s.getKey().substring(0, 10),
						meta.getContentEncoding());
			}
			return;
		}

		Mongo mongo = Configuration.getMongo();
		GridFSContentStorageWithS3 s3 = GridFSContentStorageWithS3.configure(
				mongo, props);

		if ("migrate".equals(args[1]) && "-s3".equals(args[2])) {
			DB db = mongo.getDB(Configuration.getProperty("mongo.db"));
			DBCollection contentCollection = db.getCollection("v7.fs.files");
			for (DBObject content : contentCollection.find()) {
				String store = StringUtils.defaultString(BSONUtils.getString(
						content, "store"), "raw");
				Object _id = content.get("_id");
				if (_id instanceof byte[]) {
					byte[] sha = (byte[]) _id;
					String sh = Hex.encodeHexString(sha);

					// already linked to S3?
					BSONObject existingS3 = OutOfBand.getDescriptor(content,
							"s3");
					if (existingS3 != null) {
						System.out.println("- " + sh);
						continue;
					}

					if ("raw".equals(store)) {
						try {
							if (!s3.contentAlreadyExistsInS3(sha)) {
								content.put("sha", sha);
								s3
										.insertIntoS3(
												s3.getInputStream(content),
												GridFSContentStorage
														.getLength(content),
												sha,
												GridFSContentStorage
														.getContentType(content));
							}
							BasicBSONObject alt = new BasicBSONObject();
							alt.put("store", "s3");
							alt.put("key", sha);
							s3.registerAlt(sha, alt, null, null, null);

							System.out.println("+ " + sh);
						} catch (Exception e) {
							System.err.println("ERROR " + sh + " " + e);
						}
					}
					if ("gz".equals(store)) {
						try {
							if (!s3.contentAlreadyExistsInS3(sha)) {
								content.put("sha", sha);
								s3
										.insertGzipContents(
												s3
														.getInputStreamWithGzipContents(content),
												GridFSContentStorage
														.getLength(content),
												sha,
												GridFSContentStorage
														.getContentType(content));
							}
							BasicBSONObject alt = new BasicBSONObject();
							alt.put("store", "s3");
							alt.put("key", sha);
							s3.registerAlt(sha, alt, null, null, null);

							System.out.println("+ " + sh);
						} catch (Exception e) {
							System.err.println("ERROR " + sh + " " + e);
						}
					}
				} else {
					System.err.println("? " + content);
				}
			}

			return;
		}

		if ("migrate".equals(args[1]) && "-mongo".equals(args[2])) {
			DB db = mongo.getDB(Configuration.getProperty("mongo.db"));
			DBCollection contentCollection = db.getCollection("v7.fs.files");
			GridFS gridFS = new GridFS(db, "v7.fs");
			for (DBObject content : contentCollection.find()) {
				String store = StringUtils.defaultString(BSONUtils.getString(
						content, "store"), "raw");
				try {
					byte[] sha = (byte[]) content.get("_id");
					String sh = Hex.encodeHexString(sha);

					// already in Mongo ?
					if (!"alt".equals(store)) {
						System.out.println("- " + sh);
						continue;
					}

					// not linked to S3?
					BSONObject existingS3 = OutOfBand.getDescriptor(content,
							"s3");
					if (existingS3 == null) {
						System.out.println("- " + sh);
						continue;
					}

					// already "alt-raw" ?
					if (OutOfBand.getDescriptor(content, "raw") != null) {
						System.out.println("- " + sh);
						continue;
					}

					GridFSInputFile file = gridFS.createFile(s3
							.getInputStream(existingS3));
					for (String k : content.keySet()) {
						if ("chunkSize".equals(k))
							continue;
						file.put(k, content.get(k));
					}
					List<Object> alt = new ArrayList<Object>();
					for (Object a : BSONUtils.values(content, "alt")) {
						alt.add(a);
					}
					alt.add(new BasicBSONObject("store", "raw"));
					file.put("alt", alt);
					file.put("_id", sha);
					file.save();

					System.err.println(file);
					System.out.println("+ " + sh);
				} catch (Exception e) {
					System.err.println(content);
					contentCollection.remove(content);
					e.printStackTrace();
				}
			}

			return;
		}

		System.err.println("unknown command " + args[2]);
	}

}
