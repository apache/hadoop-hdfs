/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.io.*;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.security.SecurityUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

/**
 * This class is used in Namesystem's jetty to retrieve a file.
 * Typically used by the Secondary NameNode to retrieve image and
 * edit file for periodic checkpointing.
 */
@InterfaceAudience.Private
public class GetImageServlet extends HttpServlet {
  private static final long serialVersionUID = -7669068179452648952L;

  private static final Log LOG = LogFactory.getLog(GetImageServlet.class);

  private static final String TXID_PARAMETER = "txid";
  private static final String START_TXID_PARAM = "startTxId";
  private static final String END_TXID_PARAM = "endTxId";
  
  @SuppressWarnings("unchecked")
  public void doGet(final HttpServletRequest request,
                    final HttpServletResponse response
                    ) throws ServletException, IOException {
    Map<String,String[]> pmap = request.getParameterMap();
    try {
      ServletContext context = getServletContext();
      final FSImage nnImage = (FSImage)context.getAttribute("name.system.image");
      final TransferFsImage ff = new TransferFsImage(pmap, request, response);
      final Configuration conf = 
        (Configuration)getServletContext().getAttribute(JspHelper.CURRENT_CONF);
      
      if(UserGroupInformation.isSecurityEnabled() && 
          !isValidRequestor(request.getRemoteUser(), conf)) {
        response.sendError(HttpServletResponse.SC_FORBIDDEN, 
            "Only Namenode and Secondary Namenode may access this servlet");
        LOG.warn("Received non-NN/SNN request for image or edits from " 
            + request.getRemoteHost());
        return;
      }
      
      UserGroupInformation.getCurrentUser().doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          if (ff.getImage()) {
            long txid = parseLongParam(request, TXID_PARAMETER);
            File imageFile = nnImage.getStorage().getFsImageName(txid);
            setContentLengthFromFile(response, imageFile);
            // send fsImage
            TransferFsImage.getFileServer(response.getOutputStream(), imageFile,
                getThrottler(conf)); 
          } else if (ff.getEdit()) {
            long startTxId = parseLongParam(request, START_TXID_PARAM);
            long endTxId = parseLongParam(request, END_TXID_PARAM);
            
            File editFile = nnImage.getStorage()
                .findFinalizedEditsFile(startTxId, endTxId);
            setContentLengthFromFile(response, editFile);
            
            // send edits
            TransferFsImage.getFileServer(response.getOutputStream(), editFile,
                getThrottler(conf));
          } else if (ff.putImage()) {
            final long txid = parseLongParam(request, TXID_PARAMETER);
            // TODO need some synchronization here so multiple
            // checkpointers can't upload the same image!

            // issue a HTTP get request to download the new fsimage 
            nnImage.validateCheckpointUpload(ff.getToken());
            nnImage.newImageDigest = ff.getNewChecksum();
            MD5Hash downloadImageDigest = reloginIfNecessary().doAs(
                new PrivilegedExceptionAction<MD5Hash>() {
                @Override
                public MD5Hash run() throws Exception {
                  MD5Hash digest = TransferFsImage.getFileClient(
                      ff.getInfoServer(), getParamStringForImage(txid),
                      nnImage.getStorage().getFsImageNameCheckpoint(txid), true);
                  
                  return digest;

                }
            });
            if (!nnImage.newImageDigest.equals(downloadImageDigest)) {
              throw new IOException("The downloaded image is corrupt," +
                  " expecting a checksum " + nnImage.newImageDigest +
                  " but received a checksum " + downloadImageDigest);
            }
            nnImage.checkpointUploadDone(txid, downloadImageDigest);
          }
          return null;
        }
        
        // We may have lost our ticket since the last time we tried to open
        // an http connection, so log in just in case.
        private UserGroupInformation reloginIfNecessary() throws IOException {
          // This method is only called on the NN, therefore it is safe to
          // use these key values.
          return UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                  SecurityUtil.getServerPrincipal(conf
                      .get(DFSConfigKeys.DFS_NAMENODE_KRB_HTTPS_USER_NAME_KEY),
                      NameNode.getAddress(conf).getHostName()),
              conf.get(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY));
        }       
      });
      
    } catch (Exception ie) {
      String errMsg = "GetImage failed. " + StringUtils.stringifyException(ie);
      response.sendError(HttpServletResponse.SC_GONE, errMsg);
      throw new IOException(errMsg);
    } finally {
      response.getOutputStream().close();
    }
  }
  
  /**
   * Construct a throttler from conf
   * @param conf configuration
   * @return a data transfer throttler
   */
  private final DataTransferThrottler getThrottler(Configuration conf) {
    long transferBandwidth = 
      conf.getLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY,
                   DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT);
    DataTransferThrottler throttler = null;
    if (transferBandwidth > 0) {
      throttler = new DataTransferThrottler(transferBandwidth);
    }
    return throttler;
  }
  
  @SuppressWarnings("deprecation")
  protected boolean isValidRequestor(String remoteUser, Configuration conf)
      throws IOException {
    if(remoteUser == null) { // This really shouldn't happen...
      LOG.warn("Received null remoteUser while authorizing access to getImage servlet");
      return false;
    }

    String[] validRequestors = {
        SecurityUtil.getServerPrincipal(conf
            .get(DFSConfigKeys.DFS_NAMENODE_KRB_HTTPS_USER_NAME_KEY), NameNode
            .getAddress(conf).getHostName()),
        SecurityUtil.getServerPrincipal(conf
            .get(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY), NameNode
            .getAddress(conf).getHostName()),
        SecurityUtil.getServerPrincipal(conf
            .get(DFSConfigKeys.DFS_SECONDARY_NAMENODE_KRB_HTTPS_USER_NAME_KEY),
            SecondaryNameNode.getHttpAddress(conf).getHostName()),
        SecurityUtil.getServerPrincipal(conf
            .get(DFSConfigKeys.DFS_SECONDARY_NAMENODE_USER_NAME_KEY),
            SecondaryNameNode.getHttpAddress(conf).getHostName()) };

    for(String v : validRequestors) {
      if(v != null && v.equals(remoteUser)) {
        if(LOG.isDebugEnabled()) LOG.debug("isValidRequestor is allowing: " + remoteUser);
        return true;
      }
    }
    if(LOG.isDebugEnabled()) LOG.debug("isValidRequestor is rejecting: " + remoteUser);
    return false;
  }
  
  private long parseLongParam(HttpServletRequest request, String param) throws IOException {
    // Parse the 'txid' parameter which indicates which image is to be
    // fetched.
    String paramStr = request.getParameter(param);
    if (paramStr == null) {
      throw new IOException("Invalid request has no " + param + " parameter");
    }
    
    return Long.valueOf(paramStr);
  }

  private void setContentLengthFromFile(HttpServletResponse response, File file) {
    response.setHeader(TransferFsImage.CONTENT_LENGTH,
        String.valueOf(file.length()));
  }

  static String getParamStringForImage(long txid) {
    return "getimage=1&" + TXID_PARAMETER + "=" + txid;
  }

  static String getParamStringForLog(RemoteEditLog log) {
    return "getedit=1&" + START_TXID_PARAM + "=" + log.getStartTxId()
        + "&" + END_TXID_PARAM + "=" + log.getEndTxId();
  }
}
