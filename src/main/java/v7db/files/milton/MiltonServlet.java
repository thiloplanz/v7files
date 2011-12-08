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

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;

import com.bradmcevoy.http.AuthenticationService;
import com.bradmcevoy.http.Handler;
import com.bradmcevoy.http.HttpExtension;
import com.bradmcevoy.http.http11.Http11Protocol;
import com.bradmcevoy.http.webdav.DefaultWebDavResponseHandler;

public class MiltonServlet extends com.bradmcevoy.http.MiltonServlet {

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		// http://stackoverflow.com/questions/8380324/
		httpManager.getHandlers().setEnableExpectContinue(false);

		// http://jira.ettrema.com:8080/browse/MIL-11

		handlers: for (HttpExtension x : httpManager.getHandlers()) {
			if (x instanceof Http11Protocol) {
				Http11Protocol p = (Http11Protocol) x;
				for (Handler h : x.getHandlers()) {
					if (h instanceof com.bradmcevoy.http.http11.GetHandler) {
						httpManager.addFilter(0, new GetHandler(
								new DefaultWebDavResponseHandler(
										new AuthenticationService()), p
										.getHandlerHelper()));
						break handlers;
					}
				}
			}
		}
	}

}
