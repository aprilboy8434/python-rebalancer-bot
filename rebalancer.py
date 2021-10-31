#!/usr/bin/env python
import os
import argparse
import time
import json
from decimal import Decimal, ROUND_DOWN

from apiConnection import BitKubConnection, CallServerError

import logging

class Asset(object):

    def __init__( self, name, expectedPercent ):

        self.name = name
        self.expectedPercent = expectedPercent
        self.currentPercent = None
        self.currentBalance = None
        self.expectedBalance = None
        self.currentTickerInfomationDict = None

    def __repr__( self ):
        return '{} ({}) Current Balance : {} Current Price : {}'.format(self.name, self.currentPercent, self.currentBalance, self.getCurrentPrice() ) 

    def getCurrentPrice(self):
        return self.currentTickerInfomationDict and self.currentTickerInfomationDict.get('last', None) or None

    def getCurrentAssetValue(self):
        
        assert all( [ self.currentBalance, self.getCurrentPrice() ] )
        
        return self.currentBalance * self.getCurrentPrice()


    def getAssetDiffPercent(self):

        assert all( [self.expectedPercent , self.currentPercent] )

        return abs( self.expectedPercent - self.currentPercent )


class Rebalancer(object):
    def __init__( self, bitKubConnection, assetObjList, triggerPercent, beseFiat ):
        
        self.apiConnection = bitKubConnection

        self.assetObjectList = assetObjList

        self.triggerPercent = triggerPercent

        self.beseFiat = beseFiat

        self.stopRebalance = False

    def getNetAssetValue(self):

        return sum( assetObj.getCurrentAssetValue() for assetObj in self.assetObjectList )

    def computeCurrentAsset( self ):
        ''' compute actual percent of asset
        '''
        walletBalanceDict = self.apiConnection.getWalletBalance()
        tickerInfomationDict = self.apiConnection.getTicker()

        assert all( isinstance( assetObj, Asset ) for assetObj in self.assetObjectList )
        assert sum( [ assetObj.expectedPercent for assetObj in self.assetObjectList ] ) <= 1, 'Sum expected percent of all asset must less than 1'

        #   compute actual percent
        totalPercent = 1
        for index, assetObj in enumerate( self.assetObjectList, 1 ):

            if index != len( self.assetObjectList ):
                assetObj.expectedPercent = assetObj.expectedPercent
                totalPercent -= assetObj.expectedPercent

            else:
                #   last one actual percent must equal total percent
                assetObj.expectedPercent = totalPercent

            #   set current balance
            assetObj.currentBalance = walletBalanceDict[ assetObj.name ]
            assetObj.currentTickerInfomationDict = tickerInfomationDict[ '{}_{}'.format( self.beseFiat, assetObj.name ) ]

    def rebalanceAsset( self ):

        assert self.getNetAssetValue() != 0
        
        #   loop for reblance
        doRebalance = False
        for assetObj in sorted(self.assetObjectList, key = lambda asset :asset.expectedPercent ):

            assetObj.currentPercent = assetObj.getCurrentAssetValue() / self.getNetAssetValue() 

            expectedValue = self.getNetAssetValue() * assetObj.expectedPercent

            assetObj.expectedBalance = expectedValue / assetObj.getCurrentPrice()

            if abs( assetObj.getAssetDiffPercent() ) > self.triggerPercent:
                doRebalance = True

        placeAskDictList = []
        placeBidDictList = []
        if doRebalance:
            for assetObj in sorted(self.assetObjectList, key = lambda asset :asset.expectedPercent ):
                
                commandDict = { 'sym': '{}_{}'.format( self.beseFiat, assetObj.name ),
                                'amt': None, }

                if assetObj.expectedBalance <  assetObj.currentBalance:
                    askBalance = assetObj.currentBalance - assetObj.expectedBalance
                    commandDict['amt'] = float( Decimal( str( askBalance )).quantize(Decimal('0'), rounding=ROUND_DOWN) )
                    placeAskDictList.append( commandDict )

                else:
                    bidBalance = assetObj.expectedBalance - assetObj.currentBalance
                    amount = bidBalance * assetObj.getCurrentPrice()
                    commandDict['amt'] = float( Decimal( str( amount )).quantize(Decimal('0'), rounding=ROUND_DOWN) )
                    placeBidDictList.append( commandDict )


            for placeAskDict in placeAskDictList:
                try:
                    resultDict = self.apiConnection.placeAsk( placeAskDict['sym'], placeAskDict['amt'] )
                except CallServerError as e:
                    logging.error(e)

            for placeBidDict in placeBidDictList:
                try:
                    resultDict = self.apiConnection.placeBid( placeBidDict['sym'], placeBidDict['amt'] )
                except CallServerError as e:
                    logging.error(e)

    def run( self ):
        
        #   forever loop
        while not self.stopRebalance:

            #   compute current value
            self.computeCurrentAsset()

            logging.info('Net asset value : {}'.format( self.getNetAssetValue() ))

            self.rebalanceAsset()
            
            if self.doRebalance:
                #   compute current value
                self.computeCurrentAsset()
                logging.into('Net asset value after rebalance : {}'.format( self.getNetAssetValue() ))

            #self.stopRebalance = True

            time.sleep(1)

class RebalancerConfig(object):
    ''' This class design for parser json file to python format
    '''

    def __init__( self, configJsonFilePath ):

        configJsonFilePath = os.path.expanduser( configJsonFilePath )

        #   read json file
        with open( configJsonFilePath,'r' ) as configJsonFileObj:
            data = json.load(configJsonFileObj)

        self.assetDictList = data['BALANCERS']
        self.triggerPercent = data['TRIGGER_PERCENT']
        self.interval_min = data['INTERVAL_MINUTE']
        self.beseFiat = data['BASE_FIAT']
        self.apiKey = data['API_KEY']
        self.apiSecret = data['API_SECRET']


if __name__ == '__main__':
    
    # Instantiate the parser
    parser = argparse.ArgumentParser(description='This is bot rebalancer')

    # Required positional argument
    parser.add_argument(    'configJsonFilePath', type=str,
                            help='Json configuration file'  )

    args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s [%(levelname)s] : %(funcName)s : %(message)s', level = logging.INFO)

    rebalancerConfig = RebalancerConfig( args.configJsonFilePath )

    assetObjList = [ Asset( name = assetDict['assetName'], expectedPercent=assetDict['expectedPercent'] ) for assetDict in rebalancerConfig.assetDictList ]

    bitKubConnection = BitKubConnection( rebalancerConfig.apiKey, rebalancerConfig.apiSecret )

    rebalancer = Rebalancer( bitKubConnection, assetObjList, rebalancerConfig.triggerPercent, rebalancerConfig.beseFiat )

    rebalancer.run()
