/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 * 
 * $Id$
 */
package org.dataone.client.examples;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.dataone.client.MNode;
import org.dataone.client.auth.ClientIdentityManager;
import org.dataone.client.impl.rest.MultipartCNode;
import org.dataone.client.itk.D1Client;
import org.dataone.client.types.D1TypeBuilder;
import org.dataone.ore.ResourceMapFactory;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.AccessRule;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.ObjectInfo;
import org.dataone.service.types.v1.ObjectList;
import org.dataone.service.types.v1.ObjectLocation;
import org.dataone.service.types.v1.ObjectLocationList;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.jibx.runtime.JiBXException;

/**
 * Utility for uploading data packages to a MN.  
 * 
 * Operates on a directory containing
 * data package files accompanied by system metadata documents
 * 
 * Traverses data package directories contained in PACKAGE_DIR class attribute.
 * Each directory should contain the contents of a data package including the RDF/ORE file.
 * The directory should also contain the system metadata document for each document in the package.
 * Suffix the class attribute SYSMETA_SUFFIX value (.SYSMETA) to the end of the system metadata documents.
 * 
 * Each file is uploaded to TARGET_MN_BASE_URL member node using java dataone libclient MNode class.
 * 
 * Can also be used to copy data packages from a CN to a MN using the copyDataPackages method.
 * 
 * TODO:: Check size of data files before downloading to avoid GB's of d/l and u/l.
 * 
 * @author sroseboo
 *
 */
public class ExampleDataPackageUpload {

    //private static final String TARGET_MN_BASE_URL = "https://mn-demo-5.test.dataone.org:443/knb/d1/mn";
    private static final String TARGET_MN_BASE_URL = "https://mn-sandbox-ucsb-1.test.dataone.org:443/knb/d1/mn";
    private static final String SOURCE_CN_BASE_URL = "https://cn-ucsb-1.dataone.org:443/cn";
    private static final String PACKAGE_DIR = "/tmp/";

    private static final String SYSMETA_SUFFIX = ".SYSMETA";

    public ExampleDataPackageUpload() {
    }

    /**
     * Demonstrates copying data package from a source CN to a target MN.
     * Copies data packages based on a solr query that returns resource map
     * identifiers.
     * 
     * @param args
     */
    public static void main(String[] args) {
        MNode targetMN = D1Client.getMN(TARGET_MN_BASE_URL);
        MultipartCNode sourceCN = new MultipartCNode(SOURCE_CN_BASE_URL);

        ExampleDataPackageUpload edpu = new ExampleDataPackageUpload();
        String query = buildQueryString();
        List<Identifier> oreIdentifiers = edpu.getDataPackagesToCopy(sourceCN, query);
        edpu.copyDataPackages(sourceCN, targetMN, oreIdentifiers);
        /* 
         If you have a directory containing data packages already,
         you can upload them to targetMN using:
              File folder = new File(PACKAGE_DIR);
              edpu.uploadDataPackages(mn, folder);
        */
    }

    private static String buildQueryString() {
        String baseResourceMapQuery = "?q=formatType:RESOURCE";
        //String copyByIdQuery = "id:resourceMap_107.xml";
        String daacFilter = "datasource:urn%5C:node%5C:ORNLDAAC";
        String countToCopy = "rows=2";
        String copyOffset = "start=10";
        String query = baseResourceMapQuery + "%20" + daacFilter + "&" + copyOffset + "&"
                + countToCopy;
        return query;
    }

    /**
     * Copy oreIdentifers from sourceCn to targetMN.  Each data package is downloaded to tmp directory
     * within PACKAGE_DIR along with science metadata documents.  The data package is the uploaded to
     * targetMN.
     * 
     * @param sourceCN
     * @param targetMN
     * @param oreIdentifiers
     */
    public void copyDataPackages(MultipartCNode sourceCN, MNode targetMN, List<Identifier> oreIdentifiers) {
        try {
            int packageCount = 0;
            for (Identifier orePid : oreIdentifiers) {

                File tmpDir = createTempPackageDir(packageCount);
                int docCount = 0;
                boolean downloadedAllDocs = true;

                Set<String> uniquePids = getUniqueIdsFromOre(orePid, sourceCN);
                for (String pidString : uniquePids) {
                    System.out.println("Id: " + pidString);
                    Identifier docPid = D1TypeBuilder.buildIdentifier(pidString);
                    InputStream is = getDocumentFromCnResolve(sourceCN, docPid);
                    if (is == null) {
                        System.out.println("unable to resolve pid: " + docPid.getValue()
                                + " skipping to next data package.");
                        downloadedAllDocs = false;
                        break;
                    }
                    String docFilePath = tmpDir.getAbsolutePath() + "//doc" + docCount;
                    writeDocToDir(tmpDir, is, docFilePath);
                    writeSystemMetadataToDir(sourceCN, docPid, docFilePath);
                    docCount++;
                }
                if (downloadedAllDocs) {
                    boolean success = uploadDataPackageWithRetry(targetMN, tmpDir, 5);
                    if (success) {
                        deleteTempPackageDir(tmpDir);
                    }
                } else {
                    deleteTempPackageDir(tmpDir);
                }
                packageCount++;
            }
        } catch (Exception be) {
            System.out.println(be.getMessage());
        }
    }

    /**
     * Uploads data packages in rootDir to targetMN.
     * 
     * Descends into directories contained in rootDir directory.
     * Assumes each directory within rootDir contains a data package.
     * Each data package directory should contain all documents/files
     * of the data package - including files for system metadata for each
     * part of the data package.  System metadata files are suffixed with
     * SYSMETA_SUFFIX value (".SYSMETA").
     * 
     * @param targetMN
     * @param rootDir
     */
    public void uploadDataPackages(MNode targetMN, File rootDir) {
        File[] listOfFiles = rootDir.listFiles();
        for (int i = 0; i < listOfFiles.length; i++) {
            File file = listOfFiles[i];
            if (file.isDirectory()) {
                uploadDataPackageWithRetry(targetMN, file, 10);
            }
        }
    }

    /**
     * Uploads data package in tmpDir to targetMN.
     * 
     * Each data package directory should contain all documents/files
     * of the data package - including files for system metadata for each
     * part of the data package.  System metadata files are suffixed with
     * SYSMETA_SUFFIX value (".SYSMETA").
     * 
     * @param targetMN
     * @param tmpDir
     * @param uploadRetries
     * @return
     */
    public boolean uploadDataPackageWithRetry(MNode targetMN, File tmpDir, int uploadRetries) {
        boolean success = false;
        int trycount = 0;
        do {
            success = uploadDataPackageFromDir(targetMN, tmpDir);
            if (trycount > uploadRetries) {
                System.out
                        .println("Unable to upload data package - finished retries - not deleting.");
                break;
            }
            trycount++;
        } while (!success);
        return success;
    }

    private List<Identifier> getDataPackagesToCopy(MultipartCNode cn, String queryString) {
        List<Identifier> idList = new ArrayList<Identifier>();
        ObjectList objects = new ObjectList();
        try {
            objects = cn.search("solr", queryString);
        } catch (BaseException be) {
            System.out.println(be.getMessage());
        }
        for (ObjectInfo objectInfo : objects.getObjectInfoList()) {
            idList.add(objectInfo.getIdentifier());
        }
        return idList;
    }

    private boolean uploadDataPackageFromDir(MNode mn, File packageDir) {
        File[] listOfFiles = packageDir.listFiles();
        boolean success = true;
        for (int i = 0; i < listOfFiles.length; i++) {
            File file = listOfFiles[i];
            String smdFileName = file.getName();
            String smdFilePath = file.getAbsolutePath();
            if (StringUtils.endsWith(smdFileName, SYSMETA_SUFFIX)) {
                System.out.println("found sysmeta: " + smdFilePath);
                SystemMetadata smd = unMarshalSystemMetadata(file.getAbsolutePath());
                if (smd == null) {
                    continue;
                }
                String documentFilePath = StringUtils.removeEnd(smdFilePath, SYSMETA_SUFFIX);
                File documentFile = new File(documentFilePath);
                if (documentFile.exists() == false || documentFile.isFile() == false) {
                    System.err.println("file: " + documentFilePath + " not found.");
                    continue;
                }
                smd = updateSystemMetadata(smd);
                try {
                    Identifier id = mn.create(null, smd.getIdentifier(), new FileInputStream(
                            documentFilePath), smd);
                    System.out.println("Identifier: " + id.getValue() + " created on target MN.");
                } catch (IdentifierNotUnique inu) {
                    System.out.println(inu.getMessage());
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    success = false;
                }
                System.out.println("");
            }
        }
        return success;
    }

    private SystemMetadata updateSystemMetadata(SystemMetadata smd) {
        Subject clientSubject = ClientIdentityManager.getCurrentIdentity();
        smd.setSubmitter(clientSubject);
        AccessRule ar = D1TypeBuilder.buildAccessRule("public", Permission.READ);
        smd.getAccessPolicy().addAllow(ar);
        smd.clearReplicaList();
        smd.setObsoletedBy(null);
        smd.setObsoletes(null);
        smd.setArchived(false);
        return smd;
    }

    private void writeSystemMetadataToDir(MultipartCNode cn, Identifier pid, String docFilePath)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented,
            JiBXException, FileNotFoundException, IOException {
        SystemMetadata smd = cn.getSystemMetadata(pid);
        TypeMarshaller.marshalTypeToFile(smd, docFilePath + SYSMETA_SUFFIX);
    }

    private void writeDocToDir(File tmpDir, InputStream is, String targetFilePath)
            throws FileNotFoundException, IOException {
        OutputStream os = new FileOutputStream(targetFilePath);
        IOUtils.copy(is, os);
    }

    private InputStream getDocumentFromCnResolve(MultipartCNode cn, Identifier pid) throws InvalidToken,
            ServiceFailure, NotAuthorized, NotFound, NotImplemented {
        InputStream is = null;
        ObjectLocationList oll = cn.resolve(pid);
        for (ObjectLocation location : oll.getObjectLocationList()) {
            try {
                URL url = new URL(location.getUrl());
                is = url.openConnection().getInputStream();
                break;
            } catch (IOException ioe) {
                System.out.println(ioe.getMessage());
            }
        }
        return is;
    }

    private void deleteTempPackageDir(File tmpDir) {
        File[] listOfFiles = tmpDir.listFiles();
        for (File tmpFile : listOfFiles) {
            tmpFile.delete();
        }
        tmpDir.delete();
    }

    private File createTempPackageDir(int packageCount) {
        File tmpDir = new File(PACKAGE_DIR + "//package" + packageCount);
        tmpDir.mkdir();
        return tmpDir;
    }

    private SystemMetadata unMarshalSystemMetadata(String filePath) {
        SystemMetadata smd = null;
        try {
            InputStream is = new FileInputStream(filePath);
            smd = TypeMarshaller.unmarshalTypeFromStream(SystemMetadata.class, is);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return smd;
    }

    private Set<String> getUniqueIdsFromOre(Identifier orePid, MultipartCNode cn) {
        Set<String> uniquePids = new HashSet<String>();
        try {
            InputStream oreDoc = cn.get(orePid);
            Map<Identifier, Map<Identifier, List<Identifier>>> map = ResourceMapFactory
                    .getInstance().parseResourceMap(oreDoc);
            for (Identifier id : map.keySet()) {
                uniquePids.add(id.getValue());
                Map<Identifier, List<Identifier>> documentsMap = map.get(id);
                for (Identifier documentsId : documentsMap.keySet()) {
                    uniquePids.add(documentsId.getValue());
                    List<Identifier> documentedList = documentsMap.get(documentsId);
                    for (Identifier documentedByPid : documentedList) {
                        uniquePids.add(documentedByPid.getValue());
                    }
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return uniquePids;
    }
}
