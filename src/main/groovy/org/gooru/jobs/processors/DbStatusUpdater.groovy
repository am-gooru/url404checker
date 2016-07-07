package org.gooru.jobs.processors

import groovy.util.logging.Slf4j
import groovyx.gpars.GParsPool
import org.gooru.jobs.config.Configuration
import org.gooru.jobs.db.DbConnectionHelper

import java.sql.BatchUpdateException
import java.sql.SQLException

/**
 * @author ashish on 6/7/16.
 */
@Slf4j("LOGGER")
class DbStatusUpdater {
    private static String updateStatement = '''update url_checker set http_status = :http_status, http_method = :http_method,
            xframe_options = :xframe_options, job_status = :job_status, job_ran_at = :job_ran_at, http_host = :http_host, http_port = :http_port,
            http_protocol = :http_protocol, http_path = :http_path, http_query = :http_query where id = :id'''

    def updateStatus(def resultList) {

        int subListSize = getListSize(resultList.size(), Configuration.instance.getDbPoolSize())
        LOGGER.info "For DB operation will use batch size of '{}' with '{}' threads", subListSize, Configuration.instance.getDbPoolSize()

        def inputList = resultList.collate(subListSize)

        GParsPool.withPool(Configuration.instance.getDbPoolSize()) {
            inputList.collectParallel { subInputList ->
                updateResultItemInDb(subInputList)
            }
        }
    }

    private def updateResultItemInDb(def items) {

        def sql = DbConnectionHelper.getDbConnection()
        try {
            def updated = sql.withBatch(updateStatement) { statement ->
                items.each { item ->
                    try {
                        statement.addBatch(item)
                    } catch (BatchUpdateException ex) {
                        LOGGER.warn "BatchUpdateException: Exception while updating:", ex
                        LOGGER.warn "BatchUpdateException: Next Exception is : ", ex.getNextException()
                    }
                }
            }
        } catch (SQLException e) {
            LOGGER.warn "SQLException: Not able to update status", e
            LOGGER.warn "SQLException: Next exception is : ", e.getNextException()
        } finally {
            DbConnectionHelper.closeDbConnection(sql)
        }
        items
    }

    private def getListSize(int totalSize, int availableThreads) {
        def result = totalSize % availableThreads == 0 ? totalSize/availableThreads : ((int)(totalSize/availableThreads)) + 1
        result
    }
}
