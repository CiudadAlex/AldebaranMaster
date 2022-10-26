package org.aldebaran.master.executor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.aldebaran.common.dto.JobDTO;
import org.aldebaran.common.dto.ProgressAndResultDTO;
import org.aldebaran.common.dto.RedistributionOfJARsDTO;
import org.aldebaran.common.utils.jar.JarManagementUtils;
import org.aldebaran.common.utils.lists.ListsUtils;
import org.aldebaran.common.utils.log.AppLogger;
import org.aldebaran.common.utils.log.TimeProgressLogger;
import org.aldebaran.common.utils.run.Callable;
import org.aldebaran.common.utils.run.ParallelExecutor;
import org.aldebaran.common.utils.serialization.HashUtils;
import org.aldebaran.common.utils.serialization.SerializationUtils;
import org.aldebaran.common.utils.serviceinvokers.AgentServiceInvoker;
import org.aldebaran.common.utils.text.TextUtils;
import org.aldebaran.common.utils.time.TimeUtils;

/**
 * AldebaranExecutor.
 *
 * @author Alejandro
 *
 */
public class AldebaranExecutor {

    private final String applicationName;
    private final String password;
    private final List<String> listLocationAgents;

    /**
     * Constructor for AldebaranExecutor.
     *
     * @param applicationName
     *            applicationName
     * @param password
     *            password
     * @param listLocationAgents
     *            list of locations of the agents. The location consists of
     *            <IP_or_hostname>:<port>. If no port is provided the default
     *            port (8080) will be used
     * @param pathJARs
     *            path of the JARs to distribute
     * @throws Exception
     */
    public AldebaranExecutor(final String applicationName, final String password, final List<String> listLocationAgents,
            final String pathJARs) throws Exception {
        this(applicationName, password, listLocationAgents, pathJARs, Integer.MAX_VALUE);
    }

    /**
     * Deployrt of the jars for AldebaranExecutor.
     *
     * @param applicationName
     *            applicationName
     * @param password
     *            password
     * @param listLocationAgents
     *            list of locations of the agents. The location consists of
     *            <IP_or_hostname>:<port>. If no port is provided the default
     *            port (8080) will be used
     * @param pathJARs
     *            path of the JARs to distribute
     * @param numLocationsToSend
     *            number of locations (other agents) each agent will
     *            redistribute the JARs
     * @throws Exception
     */
    public static void deploy(final String applicationName, final String password, final List<String> listLocationAgents,
            final String pathJARs, final int numLocationsToSend) throws Exception {

        new AldebaranExecutor(applicationName, password, listLocationAgents, pathJARs, numLocationsToSend);
    }

    private AldebaranExecutor(final String applicationName, final String password, final List<String> listLocationAgents,
            final String pathJARs, final int numLocationsToSend) throws Exception {

        AppLogger.logDebug("Initiating agent management");

        this.applicationName = applicationName;
        this.password = password;
        this.listLocationAgents = filterAgenstNotWorking(listLocationAgents);

        AppLogger.logDebug("Working agents: " + TextUtils.concatList(this.listLocationAgents, ", "));

        final RedistributionOfJARsDTO redistributionOfJARsDTO = JarManagementUtils.prepareJARsToSend(applicationName, pathJARs,
                this.listLocationAgents, numLocationsToSend);
        final Integer hash = HashUtils.getHash(redistributionOfJARsDTO);

        final List<String> listLocationAgentsNotUpToDate = JarManagementUtils.getAgentsNotUpToDate(password, applicationName, hash,
                this.listLocationAgents);

        if (listLocationAgentsNotUpToDate.isEmpty()) {
            AppLogger.logDebug("All agents up to date");

        } else {
            AppLogger.logDebug("Updating agents: " + TextUtils.concatList(listLocationAgentsNotUpToDate, ", "));

            final RedistributionOfJARsDTO redistributionOfJARsDTOToSend = redistributionOfJARsDTO
                    .cloneWithOtherAgents(listLocationAgentsNotUpToDate);
            JarManagementUtils.redistributeJARs(password, redistributionOfJARsDTOToSend);
        }

        AppLogger.logDebug("Update of agents finished");
    }

    private List<String> filterAgenstNotWorking(final List<String> listLocationAgents) {

        AppLogger.logDebug("Filtering agents not working");

        final List<String> listLocationAgentsWorking = new ArrayList<String>();

        for (final String locationAgent : listLocationAgents) {

            boolean isAlive = false;

            try {
                isAlive = AgentServiceInvoker.isAlive(locationAgent);

            } catch (final Exception e) {
                AppLogger.logError("Error contacting an agent", e);
            }

            if (isAlive) {
                listLocationAgentsWorking.add(locationAgent);

            } else {
                AppLogger.logDebug("Agent not working: " + locationAgent);
            }
        }

        return listLocationAgentsWorking;
    }

    /**
     * Executes the Callable objects.
     *
     * @param listJob
     *            list of the jobs to perform
     * @return list of the results in the same order
     * @throws Exception
     */
    public List<Serializable> execute(final List<Callable<?>> listJob) throws Exception {

        final Map<String, Callable<?>> mapIdJob = new HashMap<String, Callable<?>>();

        for (int i = 0; i < listJob.size(); i++) {

            final Callable<?> callable = listJob.get(i);
            mapIdJob.put("" + i, callable);
        }

        final Map<String, Serializable> mapResults = execute(mapIdJob);

        final List<Serializable> listResult = new ArrayList<Serializable>();

        for (int i = 0; i < listJob.size(); i++) {
            listResult.add(mapResults.get("" + i));
        }

        return listResult;
    }

    /**
     * Executes the Callable objects values of the map.
     *
     * @param mapIdJob
     *            map with key the id of the job and the job as a value.
     * @return map with key the id of the job and the result of the job as a
     *         value
     * @throws Exception
     */
    public Map<String, Serializable> execute(final Map<String, Callable<?>> mapIdJob) throws Exception {

        AppLogger.logDebug("Execution of jobs initiated");

        final List<String> listKeyJobs = ListsUtils.buildList(mapIdJob.keySet());
        final List<List<String>> partition = ListsUtils.splitListInPortions(listKeyJobs, listLocationAgents.size());

        final Map<String, Serializable> mapReturn = new HashMap<String, Serializable>();
        int i = 0;

        final List<Callable<Map<String, Serializable>>> listJob = new ArrayList<Callable<Map<String, Serializable>>>();
        final TimeProgressLogger progress = new TimeProgressLogger(listKeyJobs.size());

        for (final List<String> sublistKeyJobs : partition) {

            final String locationAgent = listLocationAgents.get(i);

            final Callable<Map<String, Serializable>> job = () -> {
                return execute(mapIdJob, locationAgent, sublistKeyJobs, progress);
            };

            listJob.add(job);

            i++;
        }

        final List<Map<String, Serializable>> listMapResult = ParallelExecutor.execute(listLocationAgents.size(), listJob);

        for (final Map<String, Serializable> mapResult : listMapResult) {
            mapReturn.putAll(mapResult);
        }

        AppLogger.logDebug("Execution of jobs finished");

        return mapReturn;
    }

    private Map<String, Serializable> execute(final Map<String, Callable<?>> mapIdJob, final String locationAgent,
            final List<String> sublistKeyJobs, final TimeProgressLogger progress) throws Exception {

        final Map<String, Callable<?>> mapIdJobForAgent = new HashMap<String, Callable<?>>();

        for (final String keyJob : sublistKeyJobs) {
            mapIdJobForAgent.put(keyJob, mapIdJob.get(keyJob));
        }

        final String baMapStringCallableB64 = SerializationUtils.serializeObjectToBase64(mapIdJobForAgent);

        final int totalNumberTasks = mapIdJobForAgent.keySet().size();

        final JobDTO jobDTO = new JobDTO(applicationName, baMapStringCallableB64);
        AgentServiceInvoker.executeWorker(password, jobDTO, locationAgent);

        int lastNumberTasksFinished = 0;
        String baMapStringObjectB64 = null;
        String baFailureDetailsB64 = null;

        while (baMapStringObjectB64 == null && baFailureDetailsB64 == null) {

            TimeUtils.sleep(TimeUtils.MINUTE);

            final ProgressAndResultDTO progressAndResultDTO = AgentServiceInvoker.checkProgress(password, applicationName, locationAgent);

            final int numberTasksFinished = progressAndResultDTO.getProgress();
            baMapStringObjectB64 = progressAndResultDTO.getBaMapStringObjectB64();
            baFailureDetailsB64 = progressAndResultDTO.getBaFailureDetailsB64();

            updateProgress(progress, numberTasksFinished, lastNumberTasksFinished, totalNumberTasks, baMapStringObjectB64 != null);
            lastNumberTasksFinished = numberTasksFinished;
        }

        if (baFailureDetailsB64 != null) {

            final byte[] baFailureDetails = Base64.getDecoder().decode(baFailureDetailsB64);

            AppLogger.logError("Agent in location " + locationAgent + " failed.\n\n" + new String(baFailureDetails));
            return null;

        } else {

            final Map<String, Serializable> mapResult = SerializationUtils.deserializeObjectFromBase64(baMapStringObjectB64);
            return mapResult;
        }
    }

    private void updateProgress(final TimeProgressLogger progress, final int numberTasksFinished, final int lastNumberTasksFinished,
            final int totalNumberTasks, final boolean finished) {

        final int realNumberTaskFinished = finished ? totalNumberTasks : numberTasksFinished;

        final int numToAdvance = realNumberTaskFinished - lastNumberTasksFinished;

        for (int i = 0; i < numToAdvance; i++) {
            progress.stepFinishedAndPrintProgress();
        }

    }

}
