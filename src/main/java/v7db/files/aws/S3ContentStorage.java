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

package v7db.files.aws;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;

import v7db.files.Compression;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

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
 * 
 */

public class S3ContentStorage {

	private final AmazonS3 s3;

	private final String bucketName;

	public S3ContentStorage(AmazonS3 s3, String bucketName) {
		this.s3 = s3;
		this.bucketName = bucketName;
	}

	public static S3ContentStorage configure(Properties props) {
		AmazonS3 s3 = new AmazonS3Client(
				new BasicAWSCredentials(props.getProperty("s3.accessKey"),
						props.getProperty("s3.secretKey")));
		return new S3ContentStorage(s3, props.getProperty("s3.bucket"));
	}

	public boolean contentAlreadyExists(byte[] sha) throws IOException {
		return !s3.listObjects(bucketName, Hex.encodeHexString(sha))
				.getObjectSummaries().isEmpty();
	}

	S3Object findContent(byte[] sha) throws IOException {
		String key = Hex.encodeHexString(sha);
		S3Object object = s3.getObject(bucketName, key);
		return object;
	}

	public S3Object findContentByPrefix(byte[] sha) throws IOException {
		if (sha.length == 20)
			return findContent(sha);

		if (sha.length > 20)
			throw new IllegalArgumentException();

		String prefix = Hex.encodeHexString(sha);

		ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
				.withBucketName(bucketName).withPrefix(prefix).withMaxKeys(2));

		List<S3ObjectSummary> result = objectListing.getObjectSummaries();
		if (result.isEmpty())
			return null;

		if (result.size() == 1)
			return s3.getObject(bucketName, result.get(0).getKey());

		throw new IllegalArgumentException(prefix
				+ " is not a unique SHA prefix");

	}

	private ObjectMetadata makeMetaData(long length, String filename,
			String contentType) {
		ObjectMetadata metaData = new ObjectMetadata();
		metaData.setContentLength(length);
		metaData.setContentType("application/octet-stream");
		if (StringUtils.isNotBlank(contentType))
			metaData.setContentType(contentType);
		Map<String, String> v7MetaData = new HashMap<String, String>();
		if (StringUtils.isNotBlank(filename))
			v7MetaData.put("v7-filename", filename);
		metaData.setUserMetadata(v7MetaData);
		return metaData;
	}

	private void insertGzipContents(InputStream deflatedData, long length,
			byte[] sha, String filename, String contentType) throws IOException {
		String key = Hex.encodeHexString(sha);
		ObjectMetadata metaData = makeMetaData(length, filename, contentType);
		metaData.setContentEncoding("gzip");
		s3.putObject(bucketName, key, deflatedData, metaData);
		deflatedData.close();
	}

	public byte[] insertContents(File data, String filename, String contentType)
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
						.length(), sha, filename, contentType);
				return sha;
			} finally {
				compressed.delete();
			}
		}

		String key = Hex.encodeHexString(sha);
		ObjectMetadata metaData = makeMetaData(data.length(), filename,
				contentType);
		s3.putObject(bucketName, key, new FileInputStream(data), metaData);
		return sha;

	}

	public InputStream getInputStream(byte[] sha) throws IOException {
		return getInputStream(findContent(sha));
	}

	public InputStream getInputStream(S3Object file) throws IOException {
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
