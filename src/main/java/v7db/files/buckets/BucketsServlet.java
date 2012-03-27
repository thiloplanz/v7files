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

package v7db.files.buckets;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileItemFactory;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.BSON;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import v7db.files.BSONUtils;
import v7db.files.GridFSContentStorage;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

public class BucketsServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

	private DBCollection bucketCollection;

	private GridFSContentStorage storage;

	private final BucketsServiceConfiguration properties;

	public BucketsServlet(Properties props) {
		properties = new BucketsServiceConfiguration(props);
	}

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);

		try {
			properties.init();
			bucketCollection = properties.getBucketCollection();
			storage = properties.getContentStorage();
		} catch (Exception e) {
			throw new ServletException(e);
		}
	}

	@Override
	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {

		String _id = request.getPathInfo().substring(1);

		BSONObject bucket = bucketCollection.findOne(_id);
		if (bucket == null) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND, "Bucket '"
					+ _id + "' not found");
			return;
		}

		String mode = BSONUtils.getString(bucket, "POST");
		if ("FormPost".equals(mode)) {
			doFormPost(request, response, bucket);
			return;
		}
		// method not allowed
		super.doPost(request, response);
	}

	private void doFormPost(HttpServletRequest request,
			HttpServletResponse response, BSONObject bucket) throws IOException {
		ObjectId uploadId = new ObjectId();

		BSONObject parameters = new BasicBSONObject();
		List<FileItem> files = new ArrayList<FileItem>();

		if (ServletFileUpload.isMultipartContent(request)) {
			FileItemFactory factory = new DiskFileItemFactory();
			ServletFileUpload upload = new ServletFileUpload(factory);
			try {
				for (Object _file : upload.parseRequest(request)) {
					FileItem file = (FileItem) _file;
					if (file.isFormField()) {
						String v = file.getString();
						parameters.put(file.getFieldName(), v);
					} else {
						files.add(file);
					}
				}
			} catch (FileUploadException e) {
				throw new IOException(e);
			}

		} else {
			for (Entry<String, String[]> param : request.getParameterMap()
					.entrySet()) {
				String[] v = param.getValue();
				if (v.length == 1)
					parameters.put(param.getKey(), v[0]);
				else
					parameters.put(param.getKey(), v);

			}
		}

		BSONObject result = new BasicBSONObject("_id", uploadId);
		BSONObject uploads = new BasicBSONObject();
		for (FileItem file : files) {
			DiskFileItem f = (DiskFileItem) file;
			// inline until 10KB
			if (f.isInMemory()) {
				uploads.put(f.getFieldName(), storage.insertContents(f.get(),
						10240, f.getName(), f.getContentType()));
			} else {
				uploads.put(f.getFieldName(), storage.insertContents(f
						.getStoreLocation(), 10240, f.getName(), f
						.getContentType()));
			}
			file.delete();
		}
		result.put("files", uploads);
		result.put("parameters", parameters);

		bucketCollection.update(new BasicDBObject("_id", bucket.get("_id")),
				new BasicDBObject("$push", new BasicDBObject("FormPost.data",
						result)));

		String redirect = BSONUtils.getString(bucket, "FormPost.redirect");
		// redirect mode
		if (StringUtils.isNotBlank(redirect)) {
			response.sendRedirect(redirect + "?v7_formpost_id=" + uploadId);
			return;
		}
		// echo mode

		// JSON does not work, see https://jira.mongodb.org/browse/JAVA-332
		// response.setContentType("application/json");
		// response.getWriter().write(JSON.serialize(result));
		byte[] bson = BSON.encode(result);
		response.getOutputStream().write(bson);
	}

	@Override
	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {

		String _id = request.getPathInfo().substring(1);
		byte[] sha = null;

		{
			String s = request.getParameter("sha");

			if (StringUtils.isBlank(s)) {
				response.sendError(HttpServletResponse.SC_NOT_FOUND,
						"Missing content digest");
				return;
			}

			// sha parameter
			try {
				sha = Hex.decodeHex(s.toCharArray());
			} catch (Exception e) {
				response.sendError(HttpServletResponse.SC_NOT_FOUND,
						"Invalid content digest '" + s + "'");
				return;
			}
		}

		BSONObject bucket = bucketCollection.findOne(new BasicDBObject("_id",
				_id));
		if (bucket == null) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND, "Bucket '"
					+ _id + "' not found");
			return;
		}

		String mode = BSONUtils.getString(bucket, "GET");
		if ("FormPost".equals(mode)) {
			doFormPostGet(request, response, bucket, sha);
			return;
		}
		if ("EchoPut".equals(mode)) {
			doEchoPutGet(request, response, bucket, sha);
			return;
		}
		// method not allowed
		super.doGet(request, response);

	}

	private void doFormPostGet(HttpServletRequest request,
			HttpServletResponse response, BSONObject bucket, byte[] sha)
			throws IOException {

		BSONObject file = null;

		data: for (Object o : BSONUtils.values(bucket, "FormPost.data")) {
			BSONObject upload = (BSONObject) o;
			for (Object f : BSONUtils.values(upload, "files")) {
				BSONObject bf = (BSONObject) f;
				for (String fn : bf.keySet()) {
					BSONObject x = (BSONObject) bf.get(fn);
					byte[] theSha = GridFSContentStorage.getSha(x);
					if (Arrays.equals(theSha, sha)) {
						file = x;
						break data;
					}
				}
			}
		}

		if (file == null) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND, "Bucket '"
					+ bucket.get("_id")
					+ "' does not have a file matching digest '"
					+ Hex.encodeHexString(sha) + "'");
			return;
		}

		String customFilename = request.getParameter("filename");
		if (StringUtils.isNotBlank(customFilename))
			file.put("filename", customFilename);

		sendFile(request, response, file);

	}

	private void doEchoPutGet(HttpServletRequest request,
			HttpServletResponse response, BSONObject bucket, byte[] sha)
			throws IOException {

		BSONObject file = new BasicBSONObject("sha", sha);

		if (!storage.contentAlreadyExists(sha)) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND, "Bucket '"
					+ bucket.get("_id")
					+ "' does not have a file matching digest '"
					+ Hex.encodeHexString(sha) + "'");
			return;
		}
		String customFilename = request.getParameter("filename");
		if (StringUtils.isNotBlank(customFilename))
			file.put("filename", customFilename);

		sendFile(request, response, file);

	}

	private void sendFile(HttpServletRequest request,
			HttpServletResponse response, BSONObject file) throws IOException {
		String contentType = GridFSContentStorage.getContentType(file);
		String name = GridFSContentStorage.getFilename(file);
		Long length = GridFSContentStorage.getLength(file);
		String eTag = Hex.encodeHexString(GridFSContentStorage.getSha(file));

		String ifNoneMatch = request.getHeader("If-None-Match");
		if (eTag.equals(ifNoneMatch)) {
			response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
			return;
		}

		response.setHeader("ETag", eTag);
		response.setHeader("Content-type", StringUtils.defaultString(
				contentType, "application/octet-stream"));
		if (StringUtils.isNotBlank(name))
			response.setHeader("Content-disposition", "attachment; filename=\""
					+ name + "\"");
		if (length != null)
			response.setContentLength(length.intValue());

		InputStream in = storage.getInputStream(file);
		try {
			IOUtils.copy(in, response.getOutputStream());
		} finally {
			in.close();
		}
	}

	@Override
	protected void doPut(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		String _id = request.getPathInfo().substring(1);

		BSONObject bucket = bucketCollection.findOne(_id);
		if (bucket == null) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND, "Bucket '"
					+ _id + "' not found");
			return;
		}

		String mode = BSONUtils.getString(bucket, "PUT");
		if ("EchoPut".equals(mode)) {
			doEchoPut(request, response, bucket);
			return;
		}
		// method not allowed
		super.doPut(request, response);
	}

	private void doEchoPut(HttpServletRequest request,
			HttpServletResponse response, BSONObject bucket) throws IOException {

		byte[] sha;
		File tempFile = File.createTempFile("echoPut_", ".tmp");
		try {
			FileOutputStream fos = new FileOutputStream(tempFile);
			IOUtils.copy(request.getInputStream(), fos);
			fos.close();

			sha = GridFSContentStorage.getSha(storage.insertContents(tempFile,
					0, null, null));
		} finally {
			tempFile.delete();
		}
		response.setContentType("text/plain");
		response.getWriter().write(Hex.encodeHexString(sha));
	}

}
