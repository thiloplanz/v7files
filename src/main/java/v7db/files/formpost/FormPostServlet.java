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

package v7db.files.formpost;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
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
import v7db.files.aws.GridFSContentStorageWithS3;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

public class FormPostServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

	private DBCollection controlCollection;

	private GridFSContentStorage storage;

	private final FormPostConfiguration properties;

	public FormPostServlet(Properties props) {
		properties = new FormPostConfiguration(props);
	}

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);

		try {
			properties.init();
			controlCollection = properties.getControlCollection();
			storage = properties.getContentStorage();
		} catch (Exception e) {
			throw new ServletException(e);
		}
	}

	@Override
	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {

		String _id = request.getPathInfo().substring(1);

		BSONObject control = controlCollection.findOne(_id);
		if (control == null) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND,
					"Control document '" + _id + "' not found");
			return;
		}

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

		String redirect = BSONUtils.getString(control, "redirect");
		// redirect mode
		if (StringUtils.isNotBlank(redirect)) {
			controlCollection.update(new BasicDBObject("_id", _id),
					new BasicDBObject("$push",
							new BasicDBObject("data", result)));

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

		// In the simplest case, every element is an ObjectId (data._id) for one
		// the uploads.
		// This allows a direct download using a GET request to the URL
		// <endpoint>/<_id>/<data._id>/<fieldName>/<index>.

		String[] split = StringUtils.split(request.getPathInfo(), '/');
		if (split.length < 1) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND,
					"Control document id not specified");
			return;
		}

		byte[] sha = null;
		// sha parameter
		if (StringUtils.isNotBlank(request.getParameter("sha"))) {
			String s = request.getParameter("sha");
			try {
				sha = Hex.decodeHex(s.toCharArray());
			} catch (Exception e) {
				response.sendError(HttpServletResponse.SC_NOT_FOUND,
						"Invalid content digest '" + s + "'");
				return;
			}
		}

		if (split.length < 2 && sha == null) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND,
					"Upload id not specified");
			return;
		}
		String _id = split[0];
		String upload_id = split.length > 1 ? split[1] : null;
		String fileFieldName = "file";
		ObjectId _u = null;
		if (upload_id != null)
			try {
				_u = new ObjectId(upload_id);
			} catch (Exception e) {
				response.sendError(HttpServletResponse.SC_NOT_FOUND,
						"Invalid upload id '" + upload_id + "'");
				return;
			}
		BSONObject control = controlCollection.findOne(new BasicDBObject("_id",
				_id));
		if (control == null) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND,
					"Control document '" + _id + "' not found");
			return;
		}

		BSONObject file = null;

		if (_u != null)
			for (Object f : BSONUtils.values(control, "data")) {
				if (f instanceof BSONObject) {
					BSONObject bf = (BSONObject) f;
					if (_u.equals(bf.get("_id"))) {
						f = BSONUtils.getObject(bf, "files." + fileFieldName);
						if (f instanceof BSONObject) {
							file = (BSONObject) f;
							break;
						}
					}
				}
			}

		if (sha != null) {
			if (file != null) {
				byte[] realSha = GridFSContentStorage.getSha(file);
				if (!Arrays.equals(sha, realSha)) {
					response.sendError(HttpServletResponse.SC_NOT_FOUND,
							"Content digest mismatch");
					return;
				}
			} else {
				if (storage.contentAlreadyExists(sha)) {
					file = new BasicBSONObject("sha", sha);
				}
			}
		}
		if (file == null) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND,
					"Control document '" + _id + "' does not have upload id '"
							+ upload_id + "'");
			return;
		}
		String customFilename = request.getParameter("filename");
		if (StringUtils.isNotBlank(customFilename))
			file.put("filename", customFilename);

		for (Object download : BSONUtils.values(control, "downloads")) {
			// a hex-encoded String is also allowed for ease of integration
			if (download instanceof String) {
				try {
					byte[] b = Hex.decodeHex(((String) download).toCharArray());
					// SHA-1 or ObjectId ?
					if (b.length == 20) {
						download = b;
					} else
						download = new ObjectId((String) download);
				} catch (Exception e) {
					continue;
				}
			}
			if (download instanceof ObjectId) {
				if (download.equals(_u)) {
					sendFile(request, response, file);
					return;
				}
				continue;
			}
			// SHA-1
			if (download instanceof byte[]) {
				if (download.equals(sha)) {
					sendFile(request, response, file);
					return;
				}
				continue;
			}
			if (download instanceof BSONObject) {
				BSONObject b = (BSONObject) download;
				if (BSONUtils.isTrue(b, "anySha")) {
					sendFile(request, response, file);
					return;
				}
			}
		}

		response.sendError(HttpServletResponse.SC_FORBIDDEN);
		return;

	}

	private void sendFile(HttpServletRequest request,
			HttpServletResponse response, BSONObject file) throws IOException {

		String contentType = GridFSContentStorage.getContentType(file);
		String name = GridFSContentStorage.getFilename(file);
		Long length = GridFSContentStorage.getLength(file);
		byte[] sha = GridFSContentStorage.getSha(file);
		String eTag = Hex.encodeHexString(sha);

		String ifNoneMatch = request.getHeader("If-None-Match");
		if (eTag.equals(ifNoneMatch)) {
			response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
			return;
		}

		String contentDisposition = null;
		if (StringUtils.isNotBlank(name))
			contentDisposition = "attachment; filename=\"" + name + "\"";
		if (storage instanceof GridFSContentStorageWithS3) {
			// direct download from S3, using pre-signed URL
			GridFSContentStorageWithS3 s3 = (GridFSContentStorageWithS3) storage;
			URL directDownload = s3
					.getS3DirectDownload(sha, contentDisposition);
			if (directDownload != null) {
				response.sendRedirect(directDownload.toExternalForm());
				return;
			}
		}

		response.setHeader("ETag", eTag);
		response.setHeader("Content-type", StringUtils.defaultString(
				contentType, "application/octet-stream"));
		if (StringUtils.isNotBlank(contentDisposition))
			response.setHeader("Content-disposition", contentDisposition);
		if (length != null)
			response.setContentLength(length.intValue());

		InputStream in = storage.getInputStream(file);
		try {
			IOUtils.copy(in, response.getOutputStream());
		} finally {
			in.close();
		}
	}

}
