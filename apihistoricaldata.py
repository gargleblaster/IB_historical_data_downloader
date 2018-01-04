'''
Original source came from https://gist.github.com/robcarver17/f50aeebc2ecd084f818706d9f05c1eb4

I made fairly extensive changes to make this run unattended, retrieve a whole bunch of one-minute bars for
multiple symbols over a large date range, and store the bars in appropriately named CSV files
while never violating IB's pacing violations.
It still needs more work to be more generalized, but it is getting the job done for my needs (feeding TensorFlow).
Be aware, a year of bars for two symbols can take about 24 hours.
'''

# Gist example of IB wrapper ...
#
# Download API from http://interactivebrokers.github.io/#
#
# Install python API code /IBJts/source/pythonclient $ python3 setup.py install
#
# Note: The test cases, and the documentation refer to a python package called IBApi,
#    but the actual package is called ibapi. Go figure.
#
# Get the latest version of the gateway:
# https://www.interactivebrokers.com/en/?f=%2Fen%2Fcontrol%2Fsystemstandalone-ibGateway.php%3Fos%3Dunix
#    (for unix: windows and mac users please find your own version)
#
# Run the gateway
#
# user: edemo
# pwd: demo123
#

from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.contract import Contract as IBcontract
from threading import Thread
import queue
import datetime
import time
import pandas as pd

# parameters that drive retrieval
SYMBOLS=['NQ','YM']
START_DATE = datetime.date(2016, 6, 23)
END_DATE = datetime.date(2017, 12, 31)


DEFAULT_HISTORIC_DATA_ID=50
DEFAULT_GET_CONTRACT_ID=43

## marker for when queue is finished
FINISHED = object()
STARTED = object()
TIME_OUT = object()

class finishableQueue(object):

    def __init__(self, queue_to_finish):

        self._queue = queue_to_finish
        self.status = STARTED

    def get(self, timeout):
        """
        Returns a list of queue elements once timeout is finished, or a FINISHED flag is received in the queue
        :param timeout: how long to wait before giving up
        :return: list of queue elements
        """
        contents_of_queue=[]
        finished=False

        while not finished:
            try:
                current_element = self._queue.get(timeout=timeout)
                if current_element is FINISHED:
                    finished = True
                    self.status = FINISHED
                else:
                    contents_of_queue.append(current_element)
                    ## keep going and try and get more data

            except queue.Empty:
                ## If we hit a time out it's most probable we're not getting a finished element any time soon
                ## give up and return what we have
                finished = True
                self.status = TIME_OUT


        #print('queue contents: {}'.format(contents_of_queue))
        return contents_of_queue

    def timed_out(self):
        return self.status is TIME_OUT





class MyWrapper(EWrapper):
    """
    The wrapper deals with the action coming back from the IB gateway or TWS instance
    We override methods in EWrapper that will get called when this action happens, like currentTime
    Extra methods are added as we need to store the results in this object
    """

    def __init__(self):
        self._my_contract_details = {}
        self._my_historic_data_dict = {}

    ## error handling code
    def init_error(self):
        error_queue=queue.Queue()
        self._my_errors = error_queue

    def get_error(self, timeout=5):
        if self.is_error():
            try:
                return self._my_errors.get(timeout=timeout)
            except queue.Empty:
                return None

        return None

    def is_error(self):
        an_error_if=not self._my_errors.empty()
        return an_error_if

    def error(self, id, errorCode, errorString):
        ## Overriden method
        errormsg = "IB error id %d errorcode %d string %s" % (id, errorCode, errorString)
        self._my_errors.put(errormsg)


    ## get contract details code
    def init_contractdetails(self, reqId):
        contract_details_queue = self._my_contract_details[reqId] = queue.Queue()

        return contract_details_queue

    def contractDetails(self, reqId, contractDetails):
        ## overridden method

        if reqId not in self._my_contract_details.keys():
            self.init_contractdetails(reqId)

        self._my_contract_details[reqId].put(contractDetails)

    def contractDetailsEnd(self, reqId):
        ## overriden method
        if reqId not in self._my_contract_details.keys():
            self.init_contractdetails(reqId)

        self._my_contract_details[reqId].put(FINISHED)

    ## Historic data code
    def init_historicprices(self, tickerid):
        historic_data_queue = self._my_historic_data_dict[tickerid] = queue.Queue()

        return historic_data_queue


    def historicalData(self, tickerid , bar):

        ## Overriden method
        ## Note I'm choosing to ignore barCount, WAP and hasGaps but you could use them if you like
        bardata=(bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume)
        print(bardata)

        historic_data_dict=self._my_historic_data_dict

        ## Add on to the current data
        if tickerid not in historic_data_dict.keys():
            self.init_historicprices(tickerid)

        historic_data_dict[tickerid].put(bardata)

    def historicalDataEnd(self, tickerid, start:str, end:str):
        ## overriden method

        if tickerid not in self._my_historic_data_dict.keys():
            self.init_historicprices(tickerid)

        self._my_historic_data_dict[tickerid].put(FINISHED)




class MyClient(EClient):
    """
    The client method
    We don't override native methods, but instead call them from our own wrappers
    """
    def __init__(self, wrapper):
        ## Set up with a wrapper inside
        EClient.__init__(self, wrapper)
        self.next_usable_seconds = 0

    def wait_for_pacing(self):
        now = datetime.datetime.now()
        while now.second != self.next_usable_seconds:
            print('.', end='', flush=True)
            time.sleep(1)
            now = datetime.datetime.now()
        self.last_used_seconds = datetime.datetime.now().second
        self.next_usable_seconds += 10
        if self.next_usable_seconds >= 60:
            self.next_usable_seconds = 0
        print('\nlast_used_seconds {}'.format(self.last_used_seconds))


    def resolve_ib_contract(self, ibcontract, reqId=DEFAULT_GET_CONTRACT_ID, localSymbol=None):

        """
        From a partially formed contract, returns a fully fledged version
        :returns fully resolved IB contract
        """

        ## Make a place to store the data we're going to return
        contract_details_queue = finishableQueue(self.init_contractdetails(reqId))

        print("Getting full contract details from the server... ")

        self.reqContractDetails(reqId, ibcontract)

        ## Run until we get a valid contract(s) or get bored waiting
        MAX_WAIT_SECONDS = 10
        new_contract_details = contract_details_queue.get(timeout = MAX_WAIT_SECONDS)

        while self.wrapper.is_error():
            print(self.get_error())

        if contract_details_queue.timed_out():
            print("Exceeded maximum wait for wrapper to confirm finished - seems to be normal behaviour")

        if len(new_contract_details)==0:
            print("Failed to get additional contract details: returning unresolved contract")
            return ibcontract

        if len(new_contract_details)>1:
            if localSymbol != None:
                for c in new_contract_details:
                    print('----->')
                    print(c.__dict__)
                    print('______')
                    try:
                        summary = c.summary
                        print('summary '+str(summary.__dict__))
                        print('localSymbols '+summary.localSymbol+' '+localSymbol)
                        if localSymbol == summary.localSymbol:
                            new_contract_details = c
                            print('found match for '+localSymbol)
                            break
                    except Exception as e:
                        print('no localSymbol exception ()'.format(e))
                        new_contract_details=new_contract_details[0]
            else:
                print("got multiple contracts using first one")
                new_contract_details=new_contract_details[0]
        else:
            print("got multiple contracts using only one")
            new_contract_details=new_contract_details[0]

        # failsafe
        if type(new_contract_details) is list:
            print('~~~ failsafe ~~~')
            new_contract_details = new_contract_details[0]

        resolved_ibcontract=new_contract_details.summary
        print('ibcontract: {}'.format(resolved_ibcontract))

        return resolved_ibcontract


    def get_IB_historical_data(self, ibcontract, durationStr="1 D", barSizeSetting="1 min",
                               reqId=DEFAULT_HISTORIC_DATA_ID,
                               endDateTime=None, rth=1, symbol='XX'):

        """
        Returns historical prices for a contract, up to today
        ibcontract is a Contract
        :returns list of prices in 4 tuples: Open high low close volume
        """

        if endDateTime == None:
            endDateTime = datetime.datetime.today().strftime("%Y%m%d %H:%M:%S %Z")

        print('historical endDate {}'.format(endDateTime))

        self.wait_for_pacing()

        ## Make a place to store the data we're going to return
        historic_data_queue = finishableQueue(self.init_historicprices(reqId))

        # Request some historical data. Native method in EClient
        self.reqHistoricalData(
            reqId,  # tickerId,
            ibcontract,  # contract,
            endDateTime,  # endDateTime,
            durationStr,  # durationStr,
            barSizeSetting,  # barSizeSetting,
            "BID_ASK",  # whatToShow,
            rth,  # useRTH,
            1,  # formatDate
            False,  # KeepUpToDate <<==== added for api 9.73.2
            [] ## chartoptions not used
        )


        ## Wait until we get a completed data, an error, or get bored waiting
        MAX_WAIT_SECONDS = 10
        print("Getting historical data from the server... could take %d seconds to complete " % MAX_WAIT_SECONDS)

        historic_data = historic_data_queue.get(timeout = MAX_WAIT_SECONDS)

        while self.wrapper.is_error():
            print(self.get_error())

        if historic_data_queue.timed_out():
            print("Exceeded maximum wait for wrapper to confirm finished - seems to be normal behaviour")

        self.cancelHistoricalData(reqId)

        df = pd.DataFrame(historic_data, columns=['time','o','h','l','c','v'])
        df = df.set_index('time')
        df.to_csv(symbol+'_'+endDateTime[:8]+'_'+str(rth)+'.csv')

        return historic_data



class RunApp(MyWrapper, MyClient):
    def __init__(self, ipaddress, portid, clientid):
        MyWrapper.__init__(self)
        MyClient.__init__(self, wrapper=self)

        self.connect(ipaddress, portid, clientid)

        thread = Thread(target = self.run)
        thread.start()

        setattr(self, "_thread", thread)

        self.init_error()


#if __name__ == '__main__':

app = RunApp("127.0.0.1", 4001, 3)


#resolved_ibcontract=app.resolve_ib_contract(ibcontract)
#resolved_ibcontract.includeExpired = True

# assume 1 D duration
reqId = 1
currDate = START_DATE

while currDate <= END_DATE:
    endDT = currDate - datetime.timedelta(1)
    print(endDT)

    thisYr = currDate.year

    for sym in SYMBOLS:
        print(sym, currDate)

        yearOnesDigit = thisYr % 10

        ibcontract = IBcontract()
        ibcontract.secType = "FUT+CONTFUT"
        ibcontract.symbol=sym
        ibcontract.exchange="GLOBEX"
        #ibcontract.currency="USD"
        ibcontract.includeExpired = True

        # TODO do a better job with rollover dates
        compareDate = datetime.date(thisYr,3,20)
        if currDate <= compareDate:
            ibcontract.localSymbol = '{}H{}'.format(sym, yearOnesDigit)
        else:
            compareDate = datetime.date(thisYr,6,20)
            if currDate <= compareDate:
                ibcontract.localSymbol = '{}M{}'.format(sym, yearOnesDigit)
            else:
                compareDate = datetime.date(thisYr,9,20)
                if currDate <= compareDate:
                    ibcontract.localSymbol = '{}U{}'.format(sym, yearOnesDigit)
                else:
                    compareDate = datetime.date(thisYr,12,20)
                    if currDate <= compareDate:
                        ibcontract.localSymbol = '{}Z{}'.format(sym, yearOnesDigit)
                    else:
                        # roll to next year
                        yearOnesDigit += 1
                        if yearOnesDigit > 9:
                            yearOnesDigit = 0
                        ibcontract.localSymbol = '{}H{}'.format(sym, yearOnesDigit)

        resolved_ibcontract=app.resolve_ib_contract(ibcontract, localSymbol=ibcontract.localSymbol, reqId=reqId)
        resolved_ibcontract.includeExpired = True

        reqId += 1

        try:
            historic_data = app.get_IB_historical_data(resolved_ibcontract, endDateTime=endDT.strftime('%Y%m%d 00:00:00'), reqId=reqId, symbol=sym, rth=1)
        except Exception as e:
            print('ignore exception {}'.format(e))

        reqId += 1

        try:
            historic_data = app.get_IB_historical_data(resolved_ibcontract, endDateTime=endDT.strftime('%Y%m%d 00:00:00'), reqId=reqId, symbol=sym, rth=0)
        except Exception as e:
            print('ignore exception {}'.format(e))

        print('len {}'.format(len(historic_data)))

    currDate += datetime.timedelta(1)


#print('len {}'.format(len(historic_data)))
#print(historic_data)

app.disconnect()
