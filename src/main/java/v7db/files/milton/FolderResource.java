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

import v7db.files.V7File;

import com.bradmcevoy.http.CollectionResource;
import com.bradmcevoy.http.CopyableResource;
import com.bradmcevoy.http.MakeCollectionableResource;
import com.bradmcevoy.http.PutableResource;
import com.bradmcevoy.http.Resource;
import com.bradmcevoy.http.exceptions.BadRequestException;
import com.bradmcevoy.http.exceptions.ConflictException;
import com.bradmcevoy.http.exceptions.NotAuthorizedException;

public class FolderResource extends FileResource implements CollectionResource,
		PutableResource, MakeCollectionableResource {

	FolderResource(String name, V7File file, ResourceFactory factory) {
		super(name, file, factory);
	}

	FolderResource(V7File file, ResourceFactory factory) {
		super(file, factory);
	}

	public List<? extends Resource> getChildren() {
		List<V7File> children = file.getChildren();
		List<FileResource> result = new ArrayList<FileResource>(children.size());
		for (V7File child : children) {
			if (child.hasContent())
				result.add(new FileResource(child, factory));
			else
				result.add(new FolderResource(child, factory));
		}
		return result;
	}

	public Resource createNew(String newName, InputStream inputStream,
			Long length, String contentType) throws IOException,
			ConflictException, NotAuthorizedException, BadRequestException {

		Resource existingChild = child(newName);
		if (existingChild == null) {

			V7File child = length != null ? file.createChild(inputStream,
					length, newName, contentType) : file.createChild(
					inputStream, newName, contentType);

			return new FileResource(child, factory);
		}
		if (existingChild instanceof FolderResource) {
			throw new ConflictException(existingChild,
					"already exists and is a folder");
		}

		if (length != null)
			((FileResource) existingChild).file.setContent(inputStream, length,
					contentType);
		else
			((FileResource) existingChild).file.setContent(inputStream,
					contentType);
		return existingChild;
	}

	public CollectionResource createCollection(String newName)
			throws NotAuthorizedException, ConflictException,
			BadRequestException {
		V7File child;
		try {
			child = file.createChild((byte[]) null, newName, null);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return new FolderResource(child, factory);
	}

	public Resource child(String childName) {
		V7File child = file.getChild(childName);
		if (child == null)
			return null;
		if (child.hasContent())
			return new FileResource(child, factory);
		return new FolderResource(child, factory);
	}

	@Override
	public void copyTo(CollectionResource to, String name)
			throws NotAuthorizedException, BadRequestException,
			ConflictException {
		FolderResource newParent = (FolderResource) to;
		Resource existing = newParent.child(name);
		if (existing != null)
			throw new ConflictException();
		FolderResource copy = (FolderResource) newParent.createCollection(name);
		for (Resource child : getChildren()) {
			((CopyableResource) child).copyTo(copy, child.getName());
		}
	}
}
