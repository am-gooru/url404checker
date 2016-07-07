package org.gooru.jobs.processors

import groovy.util.logging.Slf4j
import org.gooru.jobs.config.Configuration
import org.gooru.jobs.db.DbConnectionHelper

import java.sql.BatchUpdateException
import java.sql.SQLException

/**
 * @author ashish on 6/7/16.
 */
@Slf4j("LOGGER")
class DbStatusUpdater {
    private static String updateStatement = '''update url_checker set http_status = :http_status, location_redirect = :location_redirect, http_method = :http_method,
            xframe_options = :xframe_options, job_status = :job_status, job_ran_at = :job_ran_at, http_host = :http_host, http_port = :http_port,
            http_protocol = :http_protocol, http_path = :http_path, http_query = :http_query where id = :id'''

    def updateStatus(def resultList) {
        def sql = DbConnectionHelper.getDbConnection()
        try {
            sql.withBatch(Configuration.instance.getCommitBatchSize(), updateStatement) { statement ->
                resultList.each { result ->
                    try {
                        statement.addBatch(result)
                    } catch (BatchUpdateException ex) {
                        LOGGER.warn "Exception while updating:", ex
                        LOGGER.warn "Next Exception is : ", ex.getNextException()
                    }
                }
            }
        } catch (SQLException e) {
            LOGGER.warn "Not able to update status", e
            LOGGER.warn "Next exception is : ", e.getNextException()
        }
        resultList
    }
}
