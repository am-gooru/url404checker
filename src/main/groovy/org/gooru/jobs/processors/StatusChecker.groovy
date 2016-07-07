package org.gooru.jobs.processors

import groovy.util.logging.Slf4j
import org.gooru.jobs.config.Configuration

/**
 * @author ashish on 6/7/16.
 */
@Slf4j("LOGGER")
class StatusChecker {
    /*
     * When passed in a map of id and url, it actually hits the URL to find if the URL exists.
     * The processing first tries out the HEAD request. In case HEAD request is not allowed which is 405
     * then it tries with GET request
     * The return value is a List which contains following Maps of following field
     * [ id: <id>, http_status: <status>, location_redirect: <redirectLocationHeader or NULL>, http_method: <GET or HEAD whichever
     *  was successful or NULL>, xframe_options: <DENY or SAMEORIGIN or NULL>, job_status: <FAILED or COMPLETED OR null>
     *      <job_ran_at: now()>
     */

    def checkStatus(def idUrlMapInList) {
        List result = []
        idUrlMapInList.each { idUrlMap ->
            Map resultDatum = [id        : idUrlMap.id, http_status: 200, location_redirect: null, http_method: 'GET', xframe_options: null,
                               job_status: 'COMPLETED', job_ran_at: null, http_host: null, http_port: null, http_protocol: null,
                               http_query: null, http_path: null
            ]
            LOGGER.debug "Checking status of url '{}'", idUrlMap.url
            def status = checkUrlWithHead(idUrlMap.url)
            if (status.success) {
                LOGGER.debug "Status of id '{}' url '{}' is successful with HEAD", idUrlMap.id, idUrlMap.url
                resultDatum.http_status = status.http_status
                resultDatum.location_redirect = status.location_redirect
                resultDatum.http_method = 'HEAD'
                resultDatum.xframe_options = status.xframe_options
                resultDatum.job_status = 'COMPLETED'
                resultDatum.job_ran_at = new java.sql.Timestamp(System.currentTimeMillis())
                parseUrl(idUrlMap.url, resultDatum)
            } else {
                status = checkUrlWithGet(idUrlMap.url)
                if (status.success) {
                    LOGGER.debug "Status of id '{}' url '{}' is successful with GET", idUrlMap.id, idUrlMap.url
                    resultDatum.http_status = status.http_status
                    resultDatum.location_redirect = status.location_redirect
                    resultDatum.http_method = 'HEAD'
                    resultDatum.xframe_options = status.xframe_options
                    resultDatum.job_status = 'COMPLETED'
                    parseUrl(idUrlMap.url, resultDatum)
                } else {
                    LOGGER.debug "Status of id '{}' url '{}' failed", idUrlMap.id, idUrlMap.url
                    resultDatum.http_status = null
                    resultDatum.location_redirect = null
                    resultDatum.http_method = null
                    resultDatum.xframe_options = null
                    resultDatum.job_status = 'FAILED'
                    resultDatum.job_ran_at = new java.sql.Timestamp(System.currentTimeMillis())
                }
            }
            result.add(resultDatum)
        }
        result
    }

    def checkUrlWithHead(def url) {
        def status = [success: false, http_status: null, location_redirect: null, xframe_options: null]
        try {
            HttpURLConnection con =
                    (HttpURLConnection) new URL(url).openConnection()
            con.setRequestMethod("HEAD")
            con.setConnectTimeout(Configuration.instance.getHttpTimeout() * 1000)
            def responseCode
            try {
                responseCode = con.getResponseCode()
                if (responseCode == HttpURLConnection.HTTP_OK) {
                    status.success = true
                    status.http_status = responseCode
                    status.xframe_options = con.getHeaderField('X-Frame-Options')
                }  else {
                    status.success = false
                }
            } catch (java.net.SocketTimeoutException exception) {
                LOGGER.warn "Timeout happended for url '{}'", url, exception
                status.success = false
            }
        } catch (Exception e) {
            LOGGER.warn "Exception while checking with HEAD request", e
            status.success = false
        }

        status
    }

    def checkUrlWithGet(def url) {
        def status = [success: false, http_status: null, location_redirect: null, xframe_options: null]
        try {
            HttpURLConnection con =
                    (HttpURLConnection) new URL(url).openConnection()
            con.setRequestMethod("GET")
            con.setInstanceFollowRedirects(false)
            con.setConnectTimeout(Configuration.instance.getHttpTimeout() * 1000)
            def responseCode
            try {
                responseCode = con.getResponseCode()
                status.http_status = responseCode
                status.xframe_options = con.getHeaderField('X-Frame-Options')
                if (responseCode == HttpURLConnection.HTTP_OK) {
                    status.success = true
                } else {
                    status.success = false
                }
            } catch (java.net.SocketTimeoutException exception) {
                LOGGER.warn "Timeout happended for url '{}'", url, exception
                status.success = false
            }
        } catch (Exception e) {
            LOGGER.warn "Exception while checking with GET request", e
            status.success = false
        }

        status
    }

    def parseUrl(String url, Map status) {
        URL parsedUrl = new URL(url)
        status.http_port = parsedUrl.port
        status.http_protocol = parsedUrl.protocol
        status.http_host = parsedUrl.host
        status.http_path = parsedUrl.path
        status.http_query = parsedUrl.query

        LOGGER.debug "Status after parsing URL: '{}'", status

        status
    }
}
