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

import org.bson.types.ObjectId;

import v7db.files.mongodb.V7File;

import com.bradmcevoy.http.LockInfo;
import com.bradmcevoy.http.LockResult;
import com.bradmcevoy.http.LockTimeout;
import com.bradmcevoy.http.LockToken;
import com.bradmcevoy.http.LockableResource;
import com.bradmcevoy.http.exceptions.LockedException;
import com.bradmcevoy.http.exceptions.NotAuthorizedException;
import com.bradmcevoy.http.exceptions.PreConditionFailedException;

class LockableFolderResource extends FolderResource implements LockableResource {

	LockableFolderResource(V7File file, ResourceFactory factory) {
		super(file, factory);
	}

	LockableFolderResource(String endpointName, V7File file,
			ResourceFactory resourceFactory) {
		super(endpointName, file, resourceFactory);
	}

	public LockToken getCurrentLock() {
		return null;
	}

	public LockResult lock(LockTimeout timeout, LockInfo lockInfo)
			throws NotAuthorizedException, PreConditionFailedException,
			LockedException {
		return new LockResult(null, new LockToken(new ObjectId().toString(),
				lockInfo, timeout));
	}

	public LockResult refreshLock(String token) throws NotAuthorizedException,
			PreConditionFailedException {
		return null;
	}

	public void unlock(String tokenId) throws NotAuthorizedException,
			PreConditionFailedException {
	}

}
