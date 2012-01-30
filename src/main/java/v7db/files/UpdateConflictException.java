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

package v7db.files;

import static v7db.files.Vermongo._VERSION;

import com.mongodb.DBObject;

class UpdateConflictException extends Exception {

	private static final long serialVersionUID = 1L;

	private final DBObject localVersion;

	private final DBObject databaseVersion;

	UpdateConflictException(DBObject local, DBObject current) {
		this.localVersion = local;
		this.databaseVersion = current;
	}

	int getUpdateBaseVersionNumber() {
		return (Integer) localVersion.get(_VERSION);
	}

	int getConflictingCurrentVersionNumber() {
		return (Integer) databaseVersion.get(_VERSION);
	}

	Object getDocumentId() {
		return localVersion.get("_id");
	}

}
