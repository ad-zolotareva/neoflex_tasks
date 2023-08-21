import pandas as pd
import const

balance = pd.read_csv(const.balance_file_name, keep_default_na=False, delimiter=';')
balance.columns = ['id', 'ON_DATE', 'ACCOUNT_RK', 'CURRENCY_RK', 'BALANCE_OUT']

posting = pd.read_csv(const.posting_file_name, keep_default_na=False, delimiter=';')
posting.columns = ['id', 'OPER_DATE', 'CREDIT_ACCOUNT_RK', 'DEBET_ACCOUNT_RK', 'CREDIT_AMOUNT', 'DEBET_AMOUNT']

account = pd.read_csv(const.account_file_name, keep_default_na=False, delimiter=';')
account.columns = ['id', 'DATA_ACTUAL_DATE', 'DATA_ACTUAL_END_DATE', 'ACCOUNT_RK', 'ACCOUNT_NUMBER', 'CHAR_TYPE',
                   'CURRENCY_RK', 'CURRENCY_CODE']

currency = pd.read_csv(const.currency_file_name, delimiter=';', keep_default_na=False, encoding='cp866')
currency.columns = ['id', 'CURRENCY_RK', 'DATA_ACTUAL_DATE', 'DATA_ACTUAL_END_DATE', 'CURRENCY_CODE',
                    'CODE_ISO_CHAR']

exchange_rate = pd.read_csv(const.exchange_rate_file_name, keep_default_na=False, delimiter=';')
exchange_rate.columns = ['id', 'DATA_ACTUAL_DATE', 'DATA_ACTUAL_END_DATE', 'CURRENCY_RK', 'REDUCED_COURCE',
                         'CODE_ISO_NUM']

ledger_account = pd.read_csv(const.ledger_account_file_name, keep_default_na=False, delimiter=';', encoding='cp866')
ledger_account.columns = ['id', 'CHAPTER', 'CHAPTER_NAME', 'SECTION_NUMBER', 'SECTION_NAME', 'SUBSECTION_NAME',
                          'LEDGER1_ACCOUNT', 'LEDGER1_ACCOUNT_NAME', 'LEDGER_ACCOUNT', 'LEDGER_ACCOUNT_NAME',
                          'CHARACTERISTIC', 'IS_RESIDENT', 'IS_RESERVE', 'IS_RESERVED', 'IS_LOAN',
                          'IS_RESERVED_ASSETS',
                          'IS_OVERDUE', 'IS_INTEREST', 'PAIR_ACCOUNT', 'START_DATE', 'END_DATE', 'IS_RUB_ONLY',
                          'MIN_TERM', 'MIN_TERM_MEASURE', 'MAX_TERM', 'MAX_TERM_MEASURE',
                          'LEDGER_ACC_FULL_NAME_TRANSLIT', 'IS_REVALUATION', 'IS_CORRECT']
