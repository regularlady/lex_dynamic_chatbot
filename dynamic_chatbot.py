import time
import os
import logging
import json
import boto3
import uuid

# Configure logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Establish credentials
session_var = boto3.session.Session()
credentials = session_var.get_credentials()

""" --- Parameters --- """
# DynamoDB tables, read lambda function environment variables or initialize with defaults if they not exist.
DYNAMODB_PRODUCT_TABLE = os.getenv('DYNAMODB_PRODUCT_TABLE', default='reInventBootcamp-Products')
DYNAMODB_ORDER_TABLE = os.getenv('DYNAMODB_ORDER_TABLE', default='reInventBootcamp-Orders')

# Initialize DynamoDB Client
dynamodb = boto3.client('dynamodb')

##############################################################################################
# Functions

""" --- Helper functions --- """

def rreplace(s, old, new, occurrence):
    li = s.rsplit(old, occurrence)
    return new.join(li)


def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')


def convert_string_array_to_string(stringArray):
    stringList = ', '.join(map(str, stringArray))
    stringList = rreplace(stringList, ', ', ' and ', 1)
    return stringList


""" --- Generic functions used to simplify interaction with Amazon Lex --- """

def get_slots(intent_request):
    return intent_request['currentIntent']['slots']


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


def build_validation_result(is_valid, violated_slot, message_content):

    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot,
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }


""" --- Functions that interact with other services (backend functions) --- """

def get_product_types():
    """
    Called to get a list of available products.
    """
    productTypes = ['ice cream','frozen yogurt']

    return productTypes


def get_product_flavors(productType):
    """
    Called to get a list of flavors for a specific product.
    """
    productTypeFlavors = dynamodb.query(
        TableName=DYNAMODB_PRODUCT_TABLE,
        IndexName='productType-productFlavor-index',
        KeyConditionExpression='productType = :productTypeSel',
        ExpressionAttributeValues={
            ':productTypeSel' : {
                "S":productType
            }
        }
    )
    logger.debug('Available {} flavors: {}'.format(productType,json.dumps(productTypeFlavors)))
    productFlavors = []
    for productFlavor in productTypeFlavors['Items']:
        productFlavors += [str(productFlavor['productFlavor']['S']).lower()]

    return productFlavors


def get_product_id(productType,productFlavor):
    """
    Called to read the productId from DynamoDB refering to a specific flavor of a product.
    """
    productDetails = dynamodb.query(
        TableName=DYNAMODB_PRODUCT_TABLE,
        IndexName='productType-productFlavor-index',
        KeyConditionExpression='productType = :productTypeSel AND productFlavor = :productFlavorSel',
        ExpressionAttributeValues={
            ':productTypeSel' : {
                "S":productType
            },
            ':productFlavorSel' : {
                "S":productFlavor
            }
        }
    )

    if len(productDetails['Items']) != 0:
        productId = parse_int(productDetails['Items'][0]['productId']['N'])
        return productId

    return None


def validate_product_type(productType):
    """
    Called to validate the productType slot.
    """
    if productType is not None:
        productTypes = get_product_types()

        if productType.lower() not in productTypes:
            productTypesList = convert_string_array_to_string(productTypes)

            return build_validation_result(False,
                                       'productType',
                                       'We do not have {}, please select one of the following products. We offer:  '
                                       '{}'.format(productType, productTypesList))

    return build_validation_result(True, None, None)


def validate_product_flavor(productType, productFlavor):
    """
    Called to validate the productFlavor slot.
    """
    if productFlavor is not None:
        productFlavors = get_product_flavors(productType)

        if productFlavor.lower() not in productFlavors:
            productFlavorsList = convert_string_array_to_string(productFlavors)

            return build_validation_result(False,
                                       'productFlavor',
                                       'We do not have {} {}, please select one of the following {} flavors.  '
                                       '{}'.format(productFlavor, productType, productType, productFlavorsList))

    return build_validation_result(True, None, None)


def validate_order_quantity(orderQuantity):
    """
    Called to validate the orderQuantity slot.
    """
    logger.debug('Quantity: {}'.format(orderQuantity))
    if orderQuantity is not None:
        if parse_int(orderQuantity) < 5:
            return build_validation_result(False,
                        'orderQuantity',
                        'Sorry but the minimum order quantity is 5 cups. How many would you like to order?')

        elif parse_int(orderQuantity) > 30:
            return build_validation_result(False,
                        'orderQuantity',
                        'Sorry but the maximum order quantity for online orders is 30. Please contact us directly for larger quantity orders. How many cups would you like to order instead?')

    return build_validation_result(True, None, None)


def placeOrder(userId,productId,orderQuantity):
    """
    Called when the user confirms to place an order within the OrderProduct intent.
    """
    #Generate order id
    orderId = uuid.uuid4()

    logger.debug('orderId: {}, userId: {}, productId: {}, orderQuantity: {}'.format(orderId,userId,productId,orderQuantity))

    #Put order in DynamoDB
    dynamodb.put_item(
        TableName=DYNAMODB_ORDER_TABLE,
        Item={
            'orderId':{
                'S': str(orderId)
            },
            'userId':{
                'S': str(userId)
            },
            'productId':{
                'N': str(productId)
            },
            'orderQuantity':{
                'N': str(orderQuantity)
            }
        },
    )

    return orderId


""" --- Functions that control the bot's behavior (bot intent handler) --- """

def i_product_flavor(intent_request):
    """
    Called when the user triggers the GetProductFlavor intent.
    """

    source = intent_request['invocationSource']
    slots = get_slots(intent_request)

    #Slot validation
    if source == 'DialogCodeHook':
        productTypeVal = validate_product_type(slots['productType'])
        if not productTypeVal['isValid']:
            slots[productTypeVal['violatedSlot']] = None

            return elicit_slot(intent_request['sessionAttributes'],
                               intent_request['currentIntent']['name'],
                               slots,
                               productTypeVal['violatedSlot'],
                               productTypeVal['message'])

        output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
        return delegate(output_session_attributes, get_slots(intent_request))

    #Intent fulfillment
    productFlavorsList = convert_string_array_to_string(get_product_flavors(slots['productType']))

    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'Our {} offering consists of the following flavors: {}.'.format(slots['productType'],productFlavorsList)})


def i_order_product(intent_request):
    """
    Called when the user triggers the OrderProduct intent.
    """
    source = intent_request['invocationSource']

    slots = get_slots(intent_request)
    userId = intent_request['userId'] if intent_request['userId'] is not None else '0'

	#Slot validation
    if source == 'DialogCodeHook':
        if slots['productType'] is None and slots['productFlavor'] is not None:
            slots['productType'] = slots['productFlavor']

        productTypeVal = validate_product_type(slots['productType'])
        if not productTypeVal['isValid']:
            slots[productTypeVal['violatedSlot']] = None

            return elicit_slot(intent_request['sessionAttributes'],
                               intent_request['currentIntent']['name'],
                               slots,
                               productTypeVal['violatedSlot'],
                               productTypeVal['message'])

        if slots['productType'] is not None:
            productFlavorVal = validate_product_flavor(slots['productType'],slots['productFlavor'])
            if not productFlavorVal['isValid']:
                slots[productFlavorVal['violatedSlot']] = None

                return elicit_slot(intent_request['sessionAttributes'],
                                intent_request['currentIntent']['name'],
                                slots,
                                productFlavorVal['violatedSlot'],
                                productFlavorVal['message'])

        orderQuantityVal = validate_order_quantity(slots['orderQuantity'])
        if not orderQuantityVal['isValid']:
            slots[orderQuantityVal['violatedSlot']] = None

            return elicit_slot(intent_request['sessionAttributes'],
                               intent_request['currentIntent']['name'],
                               slots,
                               orderQuantityVal['violatedSlot'],
                               orderQuantityVal['message'])

        output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
        return delegate(output_session_attributes, get_slots(intent_request))

	#Intent fulfillment - Place order and confirm back to Lex

    #get productId from DynamoDB product table
    productId = get_product_id(slots['productType'],slots['productFlavor'])

    if productId is None:
        return close(intent_request['sessionAttributes'],
            'Fulfilled',
            {'contentType': 'PlainText',
            'content': 'Sorry your order of {} cups of {} {} has not been placed due to a system error. ' \
            'Please try it again later or contact us via info@reInvent.bootcamp.'.format(slots['orderQuantity'], slots['productFlavor'], slots['productType'])})

    #place order into DynamoDB order table and receive orderId in exchange
    orderId = placeOrder(userId, productId, slots['orderQuantity'])

    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'Thank you for ordering through reInvent bootcamp bot. ' \
                  'You order of {} cups of {} {} has been placed and will be processed ' \
                  'immediately (Order ID: {}). Can I help you with anything else?'.format(slots['orderQuantity'], slots['productFlavor'], slots['productType'], orderId)})


def i_help(intent_request):
    """
    Called when the user triggers the Help intent.
    """

    #Intent fulfillment
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': "Hi this is lex, your personal assistant. " \
                             "- Would you like to order something? " \
                             "- or should I show you a list of available flavors for "\
                             "one of our products?"})


""" --- Dispatch intents --- """

def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """

    logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))

    intent_name = intent_request['currentIntent']['name']

    # Dispatch to your bot's intent handlers
    if intent_name == 'GetProductFlavor':
        return i_product_flavor(intent_request)
    elif intent_name == 'OrderProduct':
        return i_order_product(intent_request)
    elif intent_name == 'Help':
        return i_help(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')


""" --- Main handler --- """

def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the Pacific timezone.
    os.environ['TZ'] = 'America/Los_Angeles'
    time.tzset()
    logger.info('Received event: {}'.format(event))

    return dispatch(event)
