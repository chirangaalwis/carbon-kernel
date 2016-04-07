/*
 *  Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.wso2.carbon.launcher.extensions;

import org.wso2.carbon.launcher.CarbonServerEvent;
import org.wso2.carbon.launcher.CarbonServerListener;
import org.wso2.carbon.launcher.Constants;
import org.wso2.carbon.launcher.bootstrap.logging.BootstrapLogger;
import org.wso2.carbon.launcher.utils.Utils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.jar.JarFile;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * DropinsBundleDeployer deploys the OSGi bundles in CARBON_HOME/osgi/dropins folder by writing the OSGi bundle
 * information to the bundles.info file.
 *
 * @since 5.0.0
 */
public class DropinsBundleDeployer implements CarbonServerListener {

    private static final Logger logger = BootstrapLogger.getCarbonLogger(DropinsBundleDeployer.class.getName());
    private static final String addOnsDirectory = "osgi";
    private static final String dropinsDirectory = "dropins";
    private static final Path bundleInfoDirectoryPath;

    static {
        String profileName = System.getProperty(Constants.PROFILE, Constants.DEFAULT_PROFILE);
        bundleInfoDirectoryPath = Paths.get(Utils.getCarbonHomeDirectory().toString(), addOnsDirectory, profileName,
                "configuration", "org.eclipse.equinox.simpleconfigurator");
    }

    /**
     * Receives notification of a CarbonServerEvent.
     *
     * @param event the CarbonServerEvent instance
     */
    @Override
    public void notify(CarbonServerEvent event) {
        if (event.getType() == CarbonServerEvent.STARTING) {
            Path dropins = Paths.get(Utils.getCarbonHomeDirectory().toString(), addOnsDirectory, dropinsDirectory);
            try {
                if (Files.exists(dropins)) {
                    List<BundleInfo> newBundleInfoLines = getNewBundleInfoLines(dropins);
                    if (revampBundlesInfo(newBundleInfoLines)) {
                        Path bundleInfoFile = Paths.get(bundleInfoDirectoryPath.toString(), "bundles.info");
                        Map<String, List<BundleInfo>> bundleInfoLineMap = processBundleInfoFile(bundleInfoFile,
                                newBundleInfoLines);
                        addNewBundleInfoLines(newBundleInfoLines, bundleInfoLineMap);
                        updateBundlesInfoFile(bundleInfoFile, bundleInfoLineMap);
                    } else {
                        logger.log(Level.INFO, "Skipped the processing of bundles.info file");
                    }
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE,
                        "An error has occurred when updating the bundles.info using the OSGi bundle information", e);
            }
        }
    }

    /**
     * Scans through the dropins directory and constructs corresponding {@code BundleInfo} instances.
     *
     * @param sourceBundleDirectory the source directory which contains the OSGi bundles
     * @return a list of {@link BundleInfo} instances
     * @throws IOException if an I/O error occurs
     */
    private static List<BundleInfo> getNewBundleInfoLines(Path sourceBundleDirectory) throws IOException {
        List<BundleInfo> existingBundleInfoLines = new ArrayList<>();
        Stream<Path> children = Files.list(sourceBundleDirectory);

        children.parallel().forEach(child -> {
            try {
                Optional<BundleInfo> newBundleInfo = getNewBundleInfoLine(child);
                if (newBundleInfo.isPresent()) {
                    existingBundleInfoLines.add(newBundleInfo.get());
                }
            } catch (IOException e) {
                logger.log(Level.WARNING, "Error when loading OSGi bundle info from " + child.toString(), e);
            }
        });
        return existingBundleInfoLines;
    }

    /**
     * Constructs a {@code BundleInfo} instance out of the OSGi bundle file path specified.
     *
     * @param filePath path to the OSGi bundle from which the {@link BundleInfo} is to be generated
     * @return a {@link BundleInfo} instance
     * @throws IOException if an I/O error occurs or if an invalid bundle is found
     */
    private static Optional<BundleInfo> getNewBundleInfoLine(Path filePath) throws IOException {
        Path path = filePath.getFileName();
        if (path != null) {
            if (path.toString().endsWith(".jar")) {
                try (JarFile jarFile = new JarFile(filePath.toString())) {
                    if ((jarFile.getManifest() == null) || (jarFile.getManifest().getMainAttributes() == null)) {
                        throw new IOException("Invalid bundle found in the " + dropinsDirectory + " directory: " +
                                jarFile.toString());
                    } else {
                        String bundleSymbolicName = jarFile.getManifest().getMainAttributes().
                                getValue("Bundle-SymbolicName");
                        String bundleVersion = jarFile.getManifest().getMainAttributes().getValue("Bundle-Version");
                        if (bundleSymbolicName == null || bundleVersion == null) {
                            logger.log(Level.WARNING,
                                    "Required bundle manifest headers do not exists: " + jarFile.toString());
                        } else {
                            //  BSN can have values like, Bundle-SymbolicName: com.example.acme;singleton:=true
                            //  refer - http://wiki.osgi.org/wiki/Bundle-SymbolicName for more details
                            if (bundleSymbolicName.contains(";")) {
                                bundleSymbolicName = bundleSymbolicName.split(";")[0];
                            }
                        }
                        //  Checking whether this bundle is a fragment or not.
                        boolean isFragment = (jarFile.getManifest().getMainAttributes().getValue("Fragment-Host")
                                != null);
                        int defaultBundleStartLevel = 4;
                        BundleInfo generated = new BundleInfo(bundleSymbolicName, bundleVersion,
                                "../../" + dropinsDirectory + "/" + path.toString(), defaultBundleStartLevel,
                                isFragment);
                        return Optional.of(generated);
                    }
                }
            } else {
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

    /**
     * Returns whether to perform a fresh processing of the bundles.info file.
     * <p>
     * The logic used for this check is as follows:
     * 1. The details of the set of bundles present in the dropins directory is stored in the file previous.info.
     * 2. If this file does not exist, a fresh revamp of the bundles.info file is carried out. A new instance of
     * the previous.info file is created and bundle information on new dropins directory bundles are added to it.
     * 3. If this file exists, bundle information on current dropins directory bundles are compared with the
     * information in the previous.info file.
     * If matching, bundles.info is not revamped. Else, bundles.info file is revamped. The existing previous.info file
     * is deleted and a new file instance is created along with bundle information on current dropins directory bundles.
     *
     * @param newBundleInfo a list of OSGi bundle information on the latest set of dropins directory bundles
     * @return true if to perform a fresh processing of the bundles.info file, else false
     * @throws IOException if an I/O error occurs
     */
    private static boolean revampBundlesInfo(List<BundleInfo> newBundleInfo) throws IOException {
        Path previousBundleInfoFile = Paths.get(bundleInfoDirectoryPath.toString(), "previous.info");
        if (Files.exists(previousBundleInfoFile)) {
            List<String> previousBundleInfo = Files.readAllLines(previousBundleInfoFile);
            if (newBundleInfo.size() == previousBundleInfo.size()) {
                long count = (newBundleInfo.stream().filter(info -> previousBundleInfo.stream().
                        filter(previousInfo -> info.toString().equals(previousInfo)).count() == 0)).count();
                if (count > 0) {
                    Files.deleteIfExists(previousBundleInfoFile);
                    List<String> bundleInfo = getBundleInfoLines(newBundleInfo);
                    Files.write(previousBundleInfoFile, bundleInfo, Charset.forName("UTF-8"));
                    return true;
                } else {
                    return false;
                }
            } else {
                Files.deleteIfExists(previousBundleInfoFile);
                List<String> bundleInfo = getBundleInfoLines(newBundleInfo);
                Files.write(previousBundleInfoFile, bundleInfo, Charset.forName("UTF-8"));
                return true;
            }
        } else {
            List<String> bundleInfo = getBundleInfoLines(newBundleInfo);
            Files.write(previousBundleInfoFile, bundleInfo, Charset.forName("UTF-8"));
            return true;
        }
    }

    /**
     * Returns a list of {@code String}s representing the {@code BundleInfo} instances.
     *
     * @param newBundleInfo the list of {@link BundleInfo} instances
     * @return a list of {@code String}s representing the {@code BundleInfo} instances
     */
    private static List<String> getBundleInfoLines(List<BundleInfo> newBundleInfo) {
        List<String> bundleInfo = new ArrayList<>();
        Optional.ofNullable(newBundleInfo).
                ifPresent(data -> data.stream().forEach(info -> bundleInfo.add(info.toString())));
        return bundleInfo;
    }

    /**
     * Returns existing OSGi bundle information by reading the bundles.info. Stale references are removed.
     * <p>
     * The mechanism used to remove stale references is as follows:
     * Information about each OSGi bundle in the existing bundles.info file is used to check if the specified
     * bundle still exists within the dropins directory. If it does not exist, the reference is removed.
     * In this mechanism if two bundle information lines are to be equal, Bundle-SymbolicNames, Bundle-Versions and
     * fragment-ness of the two bundles need to match.
     *
     * @param sourceFile         the file path to the source from which existing bundle information are to be read
     * @param newBundleInfoLines a list of {@link BundleInfo} instances corresponding to the OSGi bundles in dropins
     *                           folder
     * @return existing OSGi bundle information with stale references removed
     * @throws Exception if an error occurs when reading the bundles.info file
     */
    private static Map<String, List<BundleInfo>> processBundleInfoFile(Path sourceFile,
            List<BundleInfo> newBundleInfoLines) throws Exception {
        Map<String, List<BundleInfo>> bundleInfoLineMap = new HashMap<>();

        if (Files.exists(sourceFile)) {
            List<String> fileContent = Files.readAllLines(sourceFile, Charset.forName("UTF-8"));
            for (String line : fileContent) {
                if (!line.startsWith("#")) {
                    BundleInfo bundleInfoLine = BundleInfo.getInstance(line);
                    if (bundleInfoLine.isFromDropins()) {
                        boolean found = (newBundleInfoLines.stream().filter(newBundleInfoLine -> (
                                newBundleInfoLine.getBundleSymbolicName().equals(bundleInfoLine.getBundleSymbolicName())
                                        && newBundleInfoLine.getBundleVersion().
                                        equals(bundleInfoLine.getBundleVersion()) && (!(newBundleInfoLine.isFragment()
                                        ^ bundleInfoLine.isFragment()))))).count() == 1;
                        if (!found) {
                            //  If this dropins bundle is no longer available in the dropins directory, we remove it
                            continue;
                        }
                    }
                    List<BundleInfo> bundleInfoLines = bundleInfoLineMap.get(bundleInfoLine.getBundleSymbolicName());
                    if (bundleInfoLines == null) {
                        bundleInfoLines = new ArrayList<>();
                        bundleInfoLines.add(bundleInfoLine);
                        bundleInfoLineMap.put(bundleInfoLine.getBundleSymbolicName(), bundleInfoLines);
                    } else {
                        bundleInfoLines.add(bundleInfoLine);
                    }
                }
            }
        }
        return bundleInfoLineMap;
    }

    /**
     * Adds the new {@code BundleInfo} instance(s) to the existing OSGi bundle information.
     *
     * @param newBundleInfoLines        the list of new {@link BundleInfo} instances available in the dropins
     *                                  directory
     * @param existingBundleInfoLineMap the list of bundles currently available in the system
     */
    private static void addNewBundleInfoLines(List<BundleInfo> newBundleInfoLines,
            Map<String, List<BundleInfo>> existingBundleInfoLineMap) {
        newBundleInfoLines.forEach(newBundleInfoLine -> {
            String symbolicName = newBundleInfoLine.getBundleSymbolicName();
            String version = newBundleInfoLine.getBundleVersion();
            boolean isFragment = newBundleInfoLine.isFragment();

            List<BundleInfo> bundleInfoLineList = existingBundleInfoLineMap.get(symbolicName);

            if (bundleInfoLineList == null) {
                //  Bundle is added to the bundles.info file since it does not exist
                bundleInfoLineList = new ArrayList<>();
                bundleInfoLineList.add(newBundleInfoLine);
                existingBundleInfoLineMap.put(symbolicName, bundleInfoLineList);
                logger.log(Level.INFO, "Deploying bundle: " + newBundleInfoLine.getBundleSymbolicName() + "_" +
                        newBundleInfoLine.getBundleVersion() + ".jar");
            } else {
                boolean found = false;
                for (BundleInfo existingBundleInfoLine : bundleInfoLineList) {
                    //  Bundle symbolic names exists, hence their versions are checked for equality
                    if (existingBundleInfoLine.getBundleVersion().equals(version)) {
                        //  Compare fragment-ness, since SymbolicName and the version are matching
                        if (existingBundleInfoLine.isFragment() ^ isFragment) {
                            //  This means fragment-ness property is not equal
                            if (!existingBundleInfoLine.getBundlePath().equals(newBundleInfoLine.getBundlePath())) {
                                logger.log(Level.WARNING,
                                        "Ignoring the deployment of bundle: " + newBundleInfoLine.toString() +
                                                ", because it is already available in the system: " +
                                                existingBundleInfoLine.getBundlePath() +
                                                ". Bundle-SymbolicName and Bundle-Version headers are identical.");
                                found = true;
                                break;
                            }
                        } else {
                            //  This means fragment-ness property is equal. Seems like we have a match.
                            //  Now lets check whether their locations are equal. If the locations are equal, we don't
                            //  need to add it again. But if the locations are different we should throw a WARN.
                            if (existingBundleInfoLine.getBundlePath().equals(newBundleInfoLine.getBundlePath())) {
                                //  As we have an exact match, no need to add again
                                logger.log(Level.FINE, "Deploying bundle: " + newBundleInfoLine.getBundlePath());
                                found = true;
                                break;
                            } else {
                                // An exact match, but bundle locations are different
                                logger.log(Level.WARNING,
                                        "Ignoring the deployment of bundle: " + newBundleInfoLine.toString() +
                                                ", because it is already available in the system: " +
                                                existingBundleInfoLine.getBundlePath() +
                                                ". Bundle-SymbolicName and Bundle-Version headers are identical.");
                                found = true;
                                break;
                            }
                        }
                    } else {
                        //  Version property is different, therefore this new bundle does not exist in the system
                        found = false;
                    }
                }

                if (!found) {
                    //  Dropins bundle is not available in the system. Hence, add it.
                    bundleInfoLineList.add(newBundleInfoLine);
                    logger.log(Level.FINE, "Deploying bundle: ", newBundleInfoLine.getBundlePath());
                }
            }
        });
    }

    /**
     * Updates the bundles.info file with the new OSGi bundle information, if exists.
     *
     * @param bundlesInfoFile   the {@link Path} instance to the existing bundles.info file
     * @param bundleInfoLineMap the {@link Map} containing the complete OSGi bundle information
     * @throws Exception if an error occurs when updating the existing bundles.info file
     */
    private static void updateBundlesInfoFile(Path bundlesInfoFile, Map<String, List<BundleInfo>> bundleInfoLineMap)
            throws Exception {
        //  Generates the new bundles.info file into a temp location.
        String tempDirectory = System.getProperty("java.io.tmpdir");
        if (tempDirectory == null || tempDirectory.length() == 0) {
            throw new Exception("java.io.tmpdir property is null. Cannot proceed.");
        }

        Path tempBundlesInfoDirectory = Paths.get(tempDirectory, "bundles_info_" + UUID.randomUUID().toString());
        Path tempBundlesInfoFilePath = Paths.get(tempBundlesInfoDirectory.toString(), "bundles.info");
        if (!Files.exists(tempBundlesInfoDirectory)) {
            Files.createDirectories(tempBundlesInfoDirectory);
        }

        if (Files.exists(tempBundlesInfoDirectory)) {
            String[] keyArray = bundleInfoLineMap.keySet().toArray(new String[bundleInfoLineMap.keySet().size()]);
            Arrays.sort(keyArray);

            List<String> bundleInfoLines = new ArrayList<>();
            for (String key : keyArray) {
                List<BundleInfo> bundleInfoLineList = bundleInfoLineMap.get(key);
                bundleInfoLineList.forEach(bundleInfoLine -> bundleInfoLines.add(bundleInfoLine.toString()));
            }
            Files.write(tempBundlesInfoFilePath, bundleInfoLines, Charset.forName("UTF-8"));

            if (Files.exists(bundlesInfoFile)) {
                //  Replaces the original one with the new temporary file.
                Files.copy(tempBundlesInfoFilePath, bundlesInfoFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } else {
            throw new IOException("Failed to create the directory: " + tempBundlesInfoFilePath);
        }
    }
}
