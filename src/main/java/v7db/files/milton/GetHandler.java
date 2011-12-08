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

import static com.bradmcevoy.http.StandardFilter.INTERNAL_SERVER_ERROR_HTML;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bradmcevoy.http.Filter;
import com.bradmcevoy.http.FilterChain;
import com.bradmcevoy.http.GetableResource;
import com.bradmcevoy.http.HandlerHelper;
import com.bradmcevoy.http.HttpManager;
import com.bradmcevoy.http.Request;
import com.bradmcevoy.http.Resource;
import com.bradmcevoy.http.Response;
import com.bradmcevoy.http.ServletRequest;
import com.bradmcevoy.http.exceptions.BadRequestException;
import com.bradmcevoy.http.exceptions.ConflictException;
import com.bradmcevoy.http.exceptions.NotAuthorizedException;
import com.bradmcevoy.http.exceptions.NotFoundException;
import com.bradmcevoy.http.http11.Http11ResponseHandler;

/**
 * Support for conditional gets (If-None-Match). See
 * http://jira.ettrema.com:8080/browse/MIL-11
 * 
 */

class GetHandler extends com.bradmcevoy.http.http11.GetHandler implements
		Filter {

	private static final Logger log = LoggerFactory.getLogger(GetHandler.class);

	private final Http11ResponseHandler responseHandler;

	GetHandler(Http11ResponseHandler responseHandler,
			HandlerHelper handlerHelper) {
		super(responseHandler, handlerHelper);
		this.responseHandler = responseHandler;
	}

	@Override
	public void processExistingResource(HttpManager manager, Request request,
			Response response, Resource resource)
			throws NotAuthorizedException, BadRequestException,
			ConflictException, NotFoundException {
		if (log.isTraceEnabled()) {
			log.trace("process: " + request.getAbsolutePath());
		}
		GetableResource r = (GetableResource) resource;
		if (checkIfNoneMatch(r, request)) {
			if (log.isTraceEnabled()) {
				log.trace("respond not modified with: "
						+ responseHandler.getClass().getCanonicalName());
			}
			responseHandler.respondNotModified(r, response, request);
			return;
		}

		super.processExistingResource(manager, request, response, resource);
	}

	private boolean checkIfNoneMatch(GetableResource resource,
			Request requestInfo) {
		String header = getIfNoneMatchHeader();
		if (StringUtils.isBlank(header))
			return false;

		String etag = responseHandler.generateEtag(resource);
		if (StringUtils.isBlank(etag))
			return false;

		if (!StringUtils.contains(header, etag))
			return false;

		// TODO: proper parsing of multiple tags in the header
		return true;
	}

	private static final String getIfNoneMatchHeader() {
		HttpServletRequest request = ServletRequest.getRequest();
		return request.getHeader("If-None-Match");
	}

	public void process(FilterChain chain, Request request, Response response) {
		Request.Method method = request.getMethod();
		HttpManager manager = chain.getHttpManager();
		if (Request.Method.GET == method
				&& StringUtils.isNotBlank(getIfNoneMatchHeader())) {
			try {
				process(manager, request, response);
			} catch (BadRequestException ex) {
				log.warn("BadRequestException: " + ex.getReason());
				manager.getResponseHandler().respondBadRequest(
						ex.getResource(), response, request);
			} catch (ConflictException ex) {
				log.warn("conflictException: " + ex.getMessage());
				manager.getResponseHandler().respondConflict(ex.getResource(),
						response, request, INTERNAL_SERVER_ERROR_HTML);
			} catch (NotAuthorizedException ex) {
				log.warn("NotAuthorizedException");
				manager.getResponseHandler().respondUnauthorised(
						ex.getResource(), response, request);
			} catch (Throwable e) {
				log.error("process", e);
				try {
					manager.getResponseHandler().respondServerError(request,
							response, INTERNAL_SERVER_ERROR_HTML);
				} catch (Throwable ex) {
					log
							.error(
									"Exception generating server error response, setting response status to 500",
									ex);
					response
							.setStatus(Response.Status.SC_INTERNAL_SERVER_ERROR);
				}
			} finally {
				response.close();
			}
		} else {
			chain.process(request, response);
		}

	}

}
