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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;

import v7db.files.V7File;

import com.bradmcevoy.http.CollectionResource;
import com.bradmcevoy.http.MakeCollectionableResource;
import com.bradmcevoy.http.PutableResource;
import com.bradmcevoy.http.Resource;
import com.bradmcevoy.http.exceptions.BadRequestException;
import com.bradmcevoy.http.exceptions.ConflictException;
import com.bradmcevoy.http.exceptions.NotAuthorizedException;

public class FolderResource extends FileResource implements CollectionResource,
		PutableResource, MakeCollectionableResource {

	FolderResource(V7File file) {
		super(file);
	}

	public List<? extends Resource> getChildren() {
		V7File file = getFile();
		List<V7File> children = file.getChildren();
		List<FileResource> result = new ArrayList<FileResource>(children.size());
		for (V7File child : children) {
			if (child.hasContent())
				result.add(new FileResource(child));
			else
				result.add(new FolderResource(child));
		}
		return result;
	}

	public Resource createNew(String newName, InputStream inputStream,
			Long length, String contentType) throws IOException,
			ConflictException, NotAuthorizedException, BadRequestException {

		V7File file = getFile();
		V7File child = file.createChild(IOUtils.toByteArray(inputStream),
				newName);

		return new FileResource(child);
	}

	public CollectionResource createCollection(String newName)
			throws NotAuthorizedException, ConflictException,
			BadRequestException {
		V7File file = getFile();
		V7File child;
		try {
			child = file.createChild(null, newName);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return new FolderResource(child);
	}

}
