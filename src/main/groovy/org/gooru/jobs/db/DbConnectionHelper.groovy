package org.gooru.jobs.db

import groovy.sql.Sql
import groovy.util.logging.Slf4j
import org.gooru.jobs.config.Configuration

/**
 * @author ashish on 6/7/16.
 */
@Slf4j("LOGGER")
@Singleton
class DbConnectionHelper {

    static def getDbConnection() {
        LOGGER.debug "Opening db connection"
        def url = Configuration.instance.getDbUrl()
        def user = Configuration.instance.getDbUser()
        def password = Configuration.instance.getDbPassword()
        def driver = "org.postgresql.Driver"
        Sql.newInstance(url, user, password, driver)
    }

    static def closeDbConnection(def sql) {
        LOGGER.debug "Closing db connection"
        sql.close()
    }

}
