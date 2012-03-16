package v7db.files.aws;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.BSONObject;

import v7db.files.Compression;
import v7db.files.GridFSContentStorage;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.Mongo;

/**
 * Content storage in Amazon S3 instead of Mongo GridFS.
 * 
 * <p>
 * Content as stored in the given bucket, named as the Hex-encoded SHA id of the
 * content. It can be stored either raw or gzip-compressed, which will be
 * indicated by a Content-Encoding header (all clients should be able to handle
 * this, so it does not prevent direct downloads without any special
 * v7files-aware software).
 * 
 * <p>
 * Only the contents themselves are stored, the other metadata that usually goes
 * into the GridFS files collection remains in GridFS.
 * 
 * <p>
 * This is implemented using the OutOfBand storage scheme: A reference to S3 is
 * saved as "{ store: 's3', key: theKey }". The S3 bucket is fixed to the Mongo
 * database the current tenant or endpoint is using. Multiple databases can
 * share the same bucket, (makes garbage collection and per-tenant billing more
 * difficult, but allows for more deduplication), but a single database has only
 * one bucket.
 * 
 */

public class GridFSContentStorageWithS3 extends GridFSContentStorage {

	private final AmazonS3 s3;

	private final String bucketName;

	private GridFSContentStorageWithS3(DB db, AmazonS3 s3, String bucketName) {
		super(db);
		this.s3 = s3;
		this.bucketName = bucketName;
	}

	public static GridFSContentStorageWithS3 configure(Mongo mongo,
			Properties props) {
		AmazonS3 s3 = new AmazonS3Client(
				new BasicAWSCredentials(props.getProperty("s3.accessKey"),
						props.getProperty("s3.secretKey")));
		return new GridFSContentStorageWithS3(mongo.getDB(props
				.getProperty("mongo.db")), s3, props.getProperty("s3.bucket"));
	}

	public byte[] storeInS3(File file, String filename, Object fileId,
			String contentType) throws IOException {
		byte[] sha = insertContents(file, contentType);

		BasicDBObject result = new BasicDBObject();
		result.put("store", "s3");
		result.put("key", sha);
		registerAlt(sha, result, filename, fileId, contentType);

		return sha;
	}

	private S3Object findS3Content(byte[] sha) throws IOException {
		String key = Hex.encodeHexString(sha);
		S3Object object = s3.getObject(bucketName, key);
		return object;
	}

	private ObjectMetadata makeMetaData(long length, String contentType) {
		ObjectMetadata metaData = new ObjectMetadata();
		metaData.setContentLength(length);
		metaData.setContentType("application/octet-stream");
		if (StringUtils.isNotBlank(contentType))
			metaData.setContentType(contentType);
		Map<String, String> v7MetaData = new HashMap<String, String>();
		metaData.setUserMetadata(v7MetaData);
		return metaData;
	}

	private void insertGzipContents(InputStream deflatedData, long length,
			byte[] sha, String contentType) throws IOException {
		String key = Hex.encodeHexString(sha);
		ObjectMetadata metaData = makeMetaData(length, contentType);
		metaData.setContentEncoding("gzip");
		s3.putObject(bucketName, key, deflatedData, metaData);
		deflatedData.close();
	}

	private byte[] insertContents(File data, String contentType)
			throws IOException {

		FileInputStream fis = new FileInputStream(data);
		byte[] sha = DigestUtils.sha(fis);
		fis.close();

		if (contentAlreadyExists(sha))
			return sha;

		final File compressed = Compression.gzip(data);
		if (compressed != null) {
			try {
				insertGzipContents(new FileInputStream(compressed), compressed
						.length(), sha, contentType);
				return sha;
			} finally {
				compressed.delete();
			}
		}

		String key = Hex.encodeHexString(sha);
		ObjectMetadata metaData = makeMetaData(data.length(), contentType);
		s3.putObject(bucketName, key, new FileInputStream(data), metaData);
		return sha;

	}

	@Override
	public InputStream getInputStream(BSONObject file) throws IOException {

		if ("s3".equals(file.get("store"))) {
			return getS3InputStream((byte[]) file.get("key"));
		}
		return super.getInputStream(file);
	}

	private InputStream getS3InputStream(byte[] sha) throws IOException {
		return getS3InputStream(findS3Content(sha));
	}

	private InputStream getS3InputStream(S3Object file) throws IOException {
		if (file == null)
			return null;
		String encoding = file.getObjectMetadata().getContentEncoding();
		if ("gzip".equals(encoding))
			return new GZIPInputStream(file.getObjectContent());
		if (StringUtils.isBlank(encoding))
			return file.getObjectContent();
		throw new IllegalArgumentException("unsupported content encoding '"
				+ encoding + "'");
	}

}
