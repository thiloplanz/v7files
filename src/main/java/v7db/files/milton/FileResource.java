/**
 * Copyright (c) 2011, Thilo Planz. All rights reserved.
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

package v7db.files.milton;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import v7db.files.V7File;

import com.bradmcevoy.http.Auth;
import com.bradmcevoy.http.CollectionResource;
import com.bradmcevoy.http.GetableResource;
import com.bradmcevoy.http.MoveableResource;
import com.bradmcevoy.http.PropFindableResource;
import com.bradmcevoy.http.Range;
import com.bradmcevoy.http.Request;
import com.bradmcevoy.http.Request.Method;
import com.bradmcevoy.http.exceptions.BadRequestException;
import com.bradmcevoy.http.exceptions.ConflictException;
import com.bradmcevoy.http.exceptions.NotAuthorizedException;
import com.bradmcevoy.http.exceptions.NotFoundException;

class FileResource implements GetableResource, PropFindableResource,
		MoveableResource {

	private final V7File file;

	FileResource(V7File file) {
		this.file = file;
	}

	V7File getFile() {
		return file;
	}

	public Long getContentLength() {
		return file.getLength();
	}

	public String getContentType(String accepts) {
		return file.getContentType();
	}

	public Long getMaxAgeSeconds(Auth auth) {
		// TODO Auto-generated method stub
		return null;
	}

	public void sendContent(OutputStream out, Range range,
			Map<String, String> params, String contentType) throws IOException,
			NotAuthorizedException, BadRequestException, NotFoundException {
		InputStream content = file.getInputStream();
		if (content == null)
			throw new BadRequestException("file has no contents");
		IOUtils.copy(content, out);
	}

	public Object authenticate(String user, String password) {
		return user;
	}

	public boolean authorise(Request request, Method method, Auth auth) {
		return true;
	}

	public String checkRedirect(Request request) {
		// TODO Auto-generated method stub
		return null;
	}

	public Date getModifiedDate() {
		Date d = file.getModifiedDate();
		if (d == null)
			return file.getCreateDate();
		return d;
	}

	public String getName() {
		return file.getName();
	}

	public String getRealm() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getUniqueId() {
		return file.getDigest();
	}

	public Date getCreateDate() {
		return file.getCreateDate();
	}

	public com.bradmcevoy.http.Resource child(String childName) {
		// TODO Auto-generated method stub
		return null;
	}

	public void moveTo(CollectionResource rDest, String name)
			throws ConflictException, NotAuthorizedException,
			BadRequestException {
		try {
			file.rename(name);
		} catch (IOException e) {
			System.err.println();
			throw new ConflictException(this);
		}

	}

}
