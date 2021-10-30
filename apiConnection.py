import requests
import json
import hmac
import hashlib

AprErrorMappingDict = {
    0 :	'No error',
    1 :	'Invalid JSON payload',
    2 :	'Missing X-BTK-APIKEY',
    3 :	'Invalid API key',
    4 :	'API pending for activation',
    5 :	'IP not allowed',
    6 :	'Missing / invalid signature',
    7 :	'Missing timestamp',
    8 :	'Invalid timestamp',
    9 :	'Invalid user',
    10 :	'Invalid parameter',
    11 :	'Invalid symbol',
    12 :	'Invalid amount',
    13 :	'Invalid rate',
    14 :	'Improper rate',
    15 :	'Amount too low',
    16 :	'Failed to get balance',
    17 :	'Wallet is empty',
    18 :	'Insufficient balance',
    19 :	'Failed to insert order into db',
    20 :	'Failed to deduct balance',
    21 :	'Invalid order for cancellation',
    22 :	'Invalid side',
    23 :	'Failed to update order status',
    24 :	'Invalid order for lookup',
    25 :	'KYC level 1 is required to proceed',
    30 :	'Limit exceeds',
    40 :	'Pending withdrawal exists',
    41 :	'Invalid currency for withdrawal',
    42 :	'Address is not in whitelist',
    43 :	'Failed to deduct crypto',
    44 :	'Failed to create withdrawal record',
    45 :	'Nonce has to be numeric',
    46 :	'Invalid nonce',
    47 :	'Withdrawal limit exceeds',
    48 :	'Invalid bank account',
    49 :	'Bank limit exceeds',
    50 :	'Pending withdrawal exists',
    51 :	'Withdrawal is under maintenance',
    52 :	'Invalid permission',
    53 :	'Invalid internal address',
    54 :	'Address has been deprecated',
    90 :	'Server error (please contact support)',
}

def json_encode(data):
	return json.dumps(data, separators=(',', ':'), sort_keys=True)

def sign(apiSecret, data):
	json = json_encode(data)
	hashMassage = hmac.new(apiSecret, msg=json.encode(), digestmod=hashlib.sha256)
	return hashMassage.hexdigest()

class CallServerError( Exception ):
    pass

class ApiConnection( object ):

    def __init__( self, baseUrl, apiKey, apiSecret ):

        self.baseUrl = baseUrl
        self.apiKey = apiKey
        self.apiSecret = apiSecret.encode()

        self.postHeader = None

    def getServerTimestamp( self ):
        return self.getRequest('/api/servertime')

    def postRequest(self, api, data = {} ):

        assert self.postHeader is not None

        data['ts'] = self.getServerTimestamp()

        signature = sign(self.apiSecret, data)
        data['sig'] = signature
        response = requests.post( url = '{}{}'.format( self.baseUrl, api), data = json_encode( data ), headers = self.postHeader )

        return response.json()

    def getRequest( self, api, query = {} ):
        ''' call get then format result to dict
        '''
        response = requests.get( url = '{}{}'.format( self.baseUrl, api), params=query)
        return response.json()
    

class  BitKubConnection( ApiConnection ):

    def __init__( self, apiKey, apiSecret ):

        super().__init__(   baseUrl = 'https://api.bitkub.com',
                            apiKey = apiKey,
                            apiSecret = apiSecret )


        self.postHeader = { 'Accept': 'application/json',
                            'Content-Type': 'application/json',
                            'X-BTK-APIKEY': apiKey, }


    def getWalletBalance(self):

        api = '/api/market/wallet'

        resultDict = self.postRequest( api = api, data = {} )

        if not resultDict['error']:
            return resultDict['result']

        raise CallServerError( AprErrorMappingDict[ resultDict['error'] ] )


    def getTicker(self):

        api = '/api/market/ticker'

        return self.getRequest( api = api )

    
    def placeBid(self, symbol, amount):

        api = '/api/market/place-bid/test'

        data = {
                'sym': symbol,
                'amt': amount,
                'rat': 0,
                'typ': 'market'
        }

        resultDict = self.postRequest( api = api, data = data)
        if not resultDict['error']:
            return resultDict['result']

        raise CallServerError( AprErrorMappingDict[ resultDict['error'] ] )

    def placeAsk(self, symbol, amount):

        api = '/api/market/place-ask/test'

        data = {
            'sym': symbol,
            'amt': amount,
            'rat': 0,
            'typ': 'market'
        }
        resultDict = self.postRequest( api = api, data = data)
        if not resultDict['error']:
            return resultDict['result']

        raise CallServerError( AprErrorMappingDict[ resultDict['error'] ] )