package com.optum.rsuite.kafka.rsuiteprimetransfer.service;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.optum.rsuite.kafka.rsuiteprimetransfer.util.KafkaUtilService;
import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.model.enums.CompressionLevel;
import net.lingala.zip4j.model.enums.CompressionMethod;
import org.apache.tomcat.util.http.fileupload.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;

@Service
public class PrimeTransferService {
    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private NtlmPasswordAuthentication auth;

    @Value("${directory.manifestfile.url}")
    private String manifesturl;

    @Value("${directory.pdffile.url}")
    private String pdfurl;

    @Value("${Data_Directory}")
    private String Data_Dir;

    @Value("${RRD_SFTP_HOST}")
    private String SFTP_HOST;

    @Value("${RRD_SFTP_PORT}")
    private int SFTP_PORT;

    @Value("${RRD_SFTP_USER}")
    private String SFTP_USER;

    @Value("${RRD_SFTP_PASS}")
    private String SFTP_PASS;

    @Value("${RRD_SFTP_REMOTE_DIR}")
    private String SFTP_REMOTE_DIR;

    @Autowired
    private NotificationService notificationService;

    @KafkaListener(topics = ("${KAFKA_TRANSFER_TOPIC}"), groupId = "group_transfer", containerFactory = "outputKafkaListenerFactory")
    public void receive(byte[] payload) throws IOException {
        LOGGER.info("received payload='{}'", new String(payload));

        String payloadString = new String(payload);
        String landingZone = payloadString.substring(payloadString.indexOf(",") + 1);
        String sourceDirectory = Data_Dir + payloadString.substring(0, payloadString.indexOf(","));
        String[] listOfDir = sourceDirectory.split("/");
        String outputZipFileBase = listOfDir[listOfDir.length - 1];
        String outputZipFileDir = sourceDirectory.substring(0, sourceDirectory.indexOf(outputZipFileBase) - 1);

        if (landingZone.equalsIgnoreCase("Prime")) {
            LOGGER.info("Moving Pdfs to No Print PRODPDFs");
            MoveManifestToSMB(sourceDirectory);
            MovePDFSToSMB(sourceDirectory);

        } else if (landingZone.equalsIgnoreCase("RRD")) {
            LOGGER.info("Moving ZipFile to RRD");
            try {
                String outputZipFile = outputZipFileBase.concat(".zip");
                zipIt4J(outputZipFileDir + File.separator + outputZipFile, sourceDirectory);

                LOGGER.info("Deleting Source Directory {}", sourceDirectory);
                deleteDir(sourceDirectory);
                LOGGER.info("Source Directory deleted {}", sourceDirectory);

                copyZipToRemoteRRD(new File(outputZipFileDir + File.separator + outputZipFile), outputZipFileBase);
            } catch (IOException e) {
                LOGGER.error("Error in sending file to RRD::: [{}]", e.getMessage());
            }
            LOGGER.info("File Sent to RRD");
        }
    }

    private void MovePDFSToSMB(String sourceDirectory) {
        FileInputStream pdfInputStream = null;
        OutputStream outStream = null;
        long startTime = System.currentTimeMillis();
        try {
            File sourceDir = new File(sourceDirectory);
            int totalFiles = sourceDir.listFiles().length;
            LOGGER.info("{}.Files = {}", sourceDirectory, totalFiles);
            int processedFiles = 0;
            for (File file : sourceDir.listFiles()) {
                if (!file.getName().endsWith(".txt")) {
                    String subDirectory = file.getName().substring(10, 16);
                    SmbFile smbdirectory = new SmbFile(pdfurl + subDirectory + "//ProdPDFs//", auth);
                    if (!smbdirectory.exists()) {
                        smbdirectory.mkdirs();
                    }
                    SmbFile actualFile = new SmbFile(smbdirectory + file.getName(), auth);
                    if (!actualFile.exists()) {
                        actualFile.createNewFile();
                    }
                    outStream = actualFile.getOutputStream();
                    pdfInputStream = new FileInputStream(file);
                    IOUtils.copy(pdfInputStream, outStream);
                    processedFiles++;
                    outStream.close();
                    pdfInputStream.close();
                    LOGGER.debug("Going to delete ::: [{}]", file.getName());
                    LOGGER.debug("Deleted {} ::: [{}]", file.getName(), file.delete());
                }
            }
            LOGGER.info("Files = Total : [{}], Processed : [{}]", totalFiles, processedFiles);
            LOGGER.info("Processing completed in :" + (System.currentTimeMillis() - startTime));
            notificationService.SendMail("rsuite-prime-transfer", "INFO", "rsuite-prime-transfer update", "PDFs transferred successfully.\nSourceDirectory: [" + sourceDirectory + "]\nTotal Files: [" + totalFiles + "]\nProcessed Files: [" + processedFiles + "]");
        } catch (IOException e) {
            LOGGER.error("Error in sending PDF File to PRIME ::: [{}]", e.getMessage());
            notificationService.SendMail("rsuite-prime-transfer", "ERROR", "rsuite-prime-transfer update", "Error in sending PDF File to PRIME::: [" + e.getMessage() + "]");

        } finally {
            if (outStream != null) {
                KafkaUtilService.safeClose(outStream);
            }
            if (pdfInputStream != null) {
                KafkaUtilService.safeClose(pdfInputStream);
            }
        }
    }

    private void MoveManifestToSMB(String sourceDirectory) throws IOException {
        FileInputStream txtInputStream = null;
        OutputStream outStream = null;
        try {
            File dir = new File(sourceDirectory);
            for (File file : dir.listFiles()) {
                if (file.getName().endsWith(".txt")) {
                    LOGGER.info("Found manifest File ::: [{}]", file.getName());
                    SmbFile smbFile = new SmbFile(manifesturl + file.getName(), auth);
                    if (!smbFile.exists()) {
                        smbFile.createNewFile();
                    }
                    outStream = smbFile.getOutputStream();
                    txtInputStream = new FileInputStream(file);
                    IOUtils.copy(txtInputStream, outStream);
                    LOGGER.info("Copied [{}] file to [{}]", file.getName(), manifesturl);
                    outStream.close();
                    txtInputStream.close();
                    LOGGER.debug("Going to delete ::: [{}]", file.getName());
                    LOGGER.debug("Deleted [{}] ::: [{}]", file.getName(), file.delete());
                }
            }
        } catch (IOException e) {
            LOGGER.error("Error in sending manifest File to PRIME ::: [{}]", e.getMessage());
            notificationService.SendMail("rsuite-prime-transfer", "ERROR", "rsuite-prime-transfer update", "Error in sending manifest File to PRIME::: [" + e.getMessage() + "]");

        } finally {
            if (outStream != null) {
                KafkaUtilService.safeClose(outStream);
            }
            if (txtInputStream != null) {
                KafkaUtilService.safeClose(txtInputStream);
            }
        }
    }

    private void copyZipToRemoteRRD(File zipFile, String fileName) throws IOException {
        Session session = null;
        Channel channel = null;
        ChannelSftp channelSftp = null;
        LOGGER.info("preparing the host information for sftp.");
        FileInputStream zipStream = null;
        try {
            JSch jsch = new JSch();
            session = jsch.getSession(SFTP_USER, SFTP_HOST, SFTP_PORT);
            session.setPassword(SFTP_PASS);
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect();
            LOGGER.info("Host connected.");
            channel = session.openChannel("sftp");
            channel.connect();
            LOGGER.info("sftp channel opened and connected.");
            channelSftp = (ChannelSftp) channel;
            channelSftp.cd(SFTP_REMOTE_DIR);
            zipStream = new FileInputStream(zipFile);
            channelSftp.put(zipStream, fileName + ".zip");
            LOGGER.info("Zip File " + fileName + " transfered successfully to host.");
            notificationService.SendMail("rsuite-prime-transfer", "INFO", "rsuite-prime-transfer update", "File transferred to RRD: " + fileName + ".zip");
        } catch (Exception ex) {
            LOGGER.error("Error found while transfer the response::: [{}]", ex.getMessage());
            notificationService.SendMail("rsuite-prime-transfer", "ERROR", "rsuite-prime-transfer update", "Error found while transfer TO RRD ::: [" + ex.getMessage() + "]");
        } finally {
            channelSftp.exit();
            LOGGER.debug("sftp Channel exited.");
            channel.disconnect();
            LOGGER.debug("Channel disconnected.");
            session.disconnect();
            LOGGER.debug("Host Session disconnected.");
            zipStream.close();
        }
    }

    private void zipIt4J(String outputZipFile, String sourceDirectory) {
        ZipParameters zipParameters = new ZipParameters();
        zipParameters.setCompressionMethod(CompressionMethod.STORE);
        zipParameters.setCompressionLevel(CompressionLevel.NORMAL);

        ZipFile zipFile = new ZipFile(outputZipFile);

        LOGGER.info("Zipping Folder");

        try {
            File[] files = new File(sourceDirectory).listFiles();
            for (File file : files)
                zipFile.addFile(file);
            //zipFile.addFolder(new File(sourceDirectory));
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOGGER.info("Zipped Folder");


    }

    private boolean deleteDir(String sourceDirectory) {

        File directoryToBeDeleted = new File(sourceDirectory);
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDir(file.getAbsolutePath());
            }
        }
        return directoryToBeDeleted.delete();
    }

}
