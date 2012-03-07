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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileItemFactory;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import v7db.files.GridFSContentStorage;

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
		List<BSONObject> uploads = new ArrayList<BSONObject>();
		for (FileItem file : files) {
			DiskFileItem f = (DiskFileItem) file;
			// inline until 10KB
			if (f.isInMemory()) {
				uploads.add(storage.insertContents(f.get(), 10240, f.getName(),
						f.getContentType()));
			} else {
				uploads.add(storage.insertContents(f.getStoreLocation(), 10240,
						f.getName(), f.getContentType()));
			}
			file.delete();
		}
		result.put("files", uploads);
		result.put("parameters", parameters);
		controlCollection.update(new BasicDBObject("_id", _id),
				new BasicDBObject("$push", new BasicDBObject("data", result)));
	}
}
