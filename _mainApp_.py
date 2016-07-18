#Author: Pruthvi Kumar BK

import web,pyodbc,os,json,urllib2

urls = (
    '/*','inappropriate',
    '/v1/forex/current/?','getCurrentForexData',
    '/v1/forex/historical/?','getHistoricalForexData'
)

application = web.application(urls,globals())
web.config.debug = False

pwdForSqlEngine = os.getenv('custom_cis_sql_pwd')
driverForSqlEngine = os.getenv('custom_cis_sql_driver')
serverForSqlEngine = os.getenv('custom_cis_sql_server')
dbForSqlEngine = os.getenv('custom_cis_sql_db')
userForSqlEngine = os.getenv('custom_cis_sql_user')
connection_tunnel = 'DRIVER={'+driverForSqlEngine+'};SERVER='+serverForSqlEngine+';DATABASE='+dbForSqlEngine+';UID='+userForSqlEngine+';PWD='+pwdForSqlEngine
global cnxn_thread

class tunnel_Factory:
    def createTunnel(self):
        global cnxn_thread
        cnxn=pyodbc.connect(connection_tunnel)
        cnxn_thread = cnxn
        cursor_object=cnxn.cursor()
        return cursor_object

    def endTunnel(self,tunnelObject):
        tunnelObject.close()
        del tunnelObject
        cnxn_thread.close()

class inappropriate(object):
    def GET(self):
        responseWrapper = []
        response = {}
        response['Status']='HTTP 200'
        response['Message']='Not a valid API request. Please use /v1/forex/current/q=? for latest forex data or use /v1/forex/historical/q=? for historical.'
        responseWrapper.append(response)

        return json.dumps(responseWrapper)

class getCurrentForexData(object):
    def GET(self):
        try:

            db_init = tunnel_Factory()
            db_cursor = db_init.createTunnel()
            responseWrapper = []
            response = {}

            if(len(web.input())>=1):
                requiredForex_index = web.input().forexIndex
                a_urlParams = requiredForex_index.split(',')
                qry_iso_placeholder = '?'
                qry_iso_placeholders = ', '.join(qry_iso_placeholder for i in a_urlParams)
                qry_getCurrentForex = "SELECT cr.currencyName,er.pricingDate,er.currencyRateClose FROM dbo.ciqExchangeRate AS" \
                                      " er INNER JOIN dbo.ciqCurrency AS cr ON er.currencyId = cr.currencyId WHERE " \
                                      "(cr.ISOCode IN ({})) AND (er.latestFlag = 1)".format(qry_iso_placeholders)

                qry_currentForex_exec = db_cursor.execute(qry_getCurrentForex,tuple(a_urlParams))
                currentForex_resultSet = qry_currentForex_exec.fetchall()

                fxResultSet =[]
                for f in currentForex_resultSet:
                    fObj={}
                    fObj['currencyName'] = f[0]
                    fObj['date']=str(f[1])
                    fObj['exchangeRate']=float(f[2])
                    fxResultSet.append(fObj)

                response['Status']='HTTP 200'
                response['Message']='Successful'
                response['Success']='true'
                response['Data']=fxResultSet
                responseWrapper.append(response)


            else:
                response['Status']='HTTP 400'
                response['Message']='Invalid request. Required parameters not supplied.'
                response['Details']='This endpoint returns latest FX rates for requested indices. If required index is not passed in the URL, request cannot be completed. Format : /v1/forex/current/?forexIndex=AUD,JPY'
                responseWrapper.append(response)

            db_init.endTunnel(db_cursor)
            return json.dumps(responseWrapper)

        except :
             print('Handled in except for current endpoint. User has entered invalid request')
             return ('Invalid Request. Please revisit the URL and abide by rules to get valid response.\n\n'
                    'For latest FX data, use /v1/forex/current?forexIndex=AUD,GBP\n'
                    'For historical FX data, use /v1/forex/historical?forexIndex=AUD,GBP&startDate=2015-01-01&endDate=2016-07-01')


class getHistoricalForexData(object):
    def GET(self):
        try:
            db_init = tunnel_Factory()
            db_cursor = db_init.createTunnel()
            responseWrapper = []
            response = {}

            if(len(web.input().forexIndex)>=3 and len(web.input().startDate)==10 and len(web.input().endDate)==10):
                requiredForex_index = web.input().forexIndex
                a_urlParams = requiredForex_index.split(',')
                qry_placeholder = '?'
                qry_iso_placeholders = ', '.join(qry_placeholder for i in a_urlParams)
                startDate = str(web.input().startDate)
                endDate = str(web.input().endDate)

                qry_getHistoricalForex = "SELECT cr.currencyName,er.pricingDate,er.currencyRateClose FROM dbo.ciqExchangeRate AS" \
                                      " er INNER JOIN dbo.ciqCurrency AS cr ON er.currencyId = cr.currencyId WHERE " \
                                      "(cr.ISOCode IN ({})) AND (er.pricingDate BETWEEN ({}) and ({}))".format(qry_iso_placeholders,qry_placeholder,qry_placeholder)

                params = []
                params.extend(a_urlParams)
                params.append(startDate)
                params.append(endDate)

                qry_historicalForex_exec = db_cursor.execute(qry_getHistoricalForex,tuple(params))
                historicalForex_resultSet = qry_historicalForex_exec.fetchall()

                fxResultSet =[]
                for f in historicalForex_resultSet:
                    fObj={}
                    fObj['currencyName'] = f[0]
                    fObj['date']=str(f[1])
                    fObj['exchangeRate']=float(f[2])
                    fxResultSet.append(fObj)

                response['Status']='HTTP 200'
                response['Message']='Successful'
                response['Success']='true'
                response['Data']=fxResultSet
                responseWrapper.append(response)


            else:
                response['Status']='HTTP 400'
                response['Message']='Invalid request. Required parameters not supplied.'
                response['Details']='This endpoint returns latest FX rates for requested indices. If required index is not passed in the URL, request cannot be completed. Format : /v1/forex/historical/?forexIndex=AUD,JPY&startDate=2015-01-01&endDate=2016-07-01'
                responseWrapper.append(response)

            db_init.endTunnel(db_cursor)
            return json.dumps(responseWrapper)

        except:
            print('Handled in except for historical endpoint. User has entered invalid request')
            return ('Invalid Request. Please revisit the URL and abide by rules to get valid response.\n\n'
                    'For latest FX data, use /v1/forex/current?forexIndex=AUD,GBP\n'
                    'For historical FX data, use /v1/forex/historical?forexIndex=AUD,GBP&startDate=2015-01-01&endDate=2016-07-01')


if __name__ == '__main__':
    application.run()



