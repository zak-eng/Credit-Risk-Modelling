import requests
from urllib.request import urlretrieve
import os
import errno
from threading import Thread
import zipfile
USERNAME = 'marahzakaria03@gmail.com'
PASSWORD = '8Z~bOAJW'
START_YEAR = 2022
END_YEAR = 2023
DIR_NAME = "EDA_samples"
UNZIPPED_DIR_NAME = "unzipped_content"
login_page_url = 'https://freddiemac.embs.com/FLoan/secure/auth.php'
download_page_url = 'https://freddiemac.embs.com/FLoan/Data/download.php'
REMOVE_UNZIPPED_FILES = False
def get_start_year():
    return START_YEAR
def create_files():
    output_file=str(START_YEAR)+"_"+str(END_YEAR)+"_orig.csv"
    header = [
        "Credit Score", "First Payment Date", "First Time Homebuyer Flag", "Maturity Date",
        "Metropolitan Statistical Area (MSA) Or Metropolitan Division", "Mortgage Insurance Percentage (MI %)",
        "Number of Units", "Occupancy Status", "Original Combined Loan-to-Value (CLTV)",
        "Original Debt-to-Income (DTI) Ratio", "Original UPB", "Original Loan-to-Value (LTV)",
        "Original Interest Rate", "Channel", "Prepayment Penalty Mortgage (PPM) Flag",
        "Amortization Type (Formerly Product Type)", "Property State", "Property Type",
        "Postal Code", "Loan Sequence Number", "Loan Purpose", "Original Loan Term",
        "Number of Borrowers", "Seller Name", "Servicer Name", "Super Conforming Flag",
        "Pre-HARP Loan Sequence Number", "Program Indicator", "HARP Indicator",
        "Property Valuation Method", "Interest Only (I/O) Indicator", "Mortgage Insurance Cancellation Indicator"
    ]
    with open(output_file, 'w') as destination:
          destination.write(','.join(header) + '\n')
    output_file = str(START_YEAR) + "_" + str(END_YEAR) + "_svcg.csv"
    headers = [
        "Loan Sequence Number",
        "Monthly Reporting Period",
        "Current Actual UPB",
        "Current Loan Delinquency Status",
        "Loan Age",
        "Remaining Months to Legal Maturity",
        "Defect Settlement Date",
        "Modification Flag",
        "Zero Balance Code",
        "Zero Balance Effective Date",
        "Current Interest Rate",
        "Current Deferred UPB",
        "Due Date of Last Paid Installment (DDLPI)",
        "MI Recoveries",
        "Net Sales Proceeds",
        "Non MI Recoveries",
        "Expenses",
        "Legal Costs",
        "Maintenance and Preservation Costs",
        "Taxes and Insurance",
        "Miscellaneous Expenses",
        "Actual Loss Calculation",
        "Modification Cost",
        "Step Modification Flag",
        "Deferred Payment Plan",
        "Estimated Loan-to-Value (ELTV)",
        "Zero Balance Removal UPB",
        "Delinquent Accrued Interest",
        "Delinquency Due to Disaster",
        "Borrower Assistance Status Code",
        "Current Month Modification Cost",
        "Interest Bearing UPB"
    ]
    with open(output_file, 'w') as destination:
        destination.write(','.join(headers) + '\n')

def transform_orig_to_csv(year, output_file):
    with open(UNZIPPED_DIR_NAME + "/sample_orig"  + "_" + str(year) + '.txt') as source, open(output_file, 'a') as destination:

        for line in source:
            # Replace pipe ('|') with a comma (',')
            csv_line = line.replace(",", "_")
            csv_line = csv_line.replace('|', ',')

            csv_line = csv_line.replace(",\n", "\n")
            # Write the transformed line to the destination file
            destination.write(csv_line)

    print(f"Transformation complete. Saved CSV to {output_file}")

def transform_svcg_to_csv(year, output_file):

    with open(UNZIPPED_DIR_NAME + "/sample_svcg"  + "_" + str(year) + '.txt') as source, open(output_file, 'a') as destination:
        for line in source:
            # Replace pipe ('|') with a comma (',')
            csv_line = line.replace(",", "_")
            csv_line = csv_line.replace('|', ',')

            csv_line = csv_line.replace(",\n", "\n")
            # Write the transformed line to the destination file
            destination.write(csv_line)

    print(f"Transformation complete. Saved CSV to {output_file}")



def get_data_from_url(year):
    create_files()
    download = urlretrieve('https://freddiemac.embs.com/FLoan/Data/sample_' + str(year) + '.zip',
                           DIR_NAME + '/sample_' + str(year) + '.zip')
    print(" SUCCESS   : sample_" + str(year) + ".zip download complete ")
    # unzip_files(year)
    try:
        zip_ref = zipfile.ZipFile(DIR_NAME + "/sample_" + str(year) + '.zip', 'r')
        zip_ref.extractall(UNZIPPED_DIR_NAME)
        zip_ref.close()
        print(" SUCCESS   : sample_" + str(year) + ".zip unzip complete ")
        os.remove(DIR_NAME + "/sample_" + str(year) + '.zip')
        print(" SUCCESS   : sample_" + str(year) + ".zip deletion complete ")
        #transform_orig_to_csv(year, str(START_YEAR)+"_"+str(END_YEAR)+"_orig.csv")
        transform_svcg_to_csv(year, str(START_YEAR)+"_"+str(END_YEAR)+"_svcg.csv")
        #(Thread(target=write_into_consolidated_file, args=(year, create_consolidated_file('orig'),))).start()
        #(Thread(target=write_into_consolidated_file, args=(year, create_consolidated_file('svcg'),))).start()
    except zipfile.BadZipfile:
        print(" ERROR   : Unsuccesfull to unzip "+DIR_NAME + "/sample_" + str(year) + '.zip')
        print (zipfile.BadZipfile)
def create_directory(dir_name):
    curr_dir = os.getcwd()
    if not os.path.exists(dir_name):
        try:
            os.makedirs(dir_name)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
    print(" SUCCESS   : "+dir_name+" directory creation succesful ")


def start_execution():
    with requests.Session() as sess:
          print(" SUCCESS   : Program Started Succesfully")
          sess.get(login_page_url);
          php_session_cookie = sess.cookies['PHPSESSID']
          login_payload = {'username' : USERNAME, 'password' : PASSWORD,'cookie':php_session_cookie}
          response_login = sess.post(login_page_url, data = login_payload)
          download_page_payload = {'accept': 'Yes', 'action': 'acceptTandC', 'acceptSubmit': 'Continue', 'cookie': php_session_cookie}
          response_download = sess.post(download_page_url, data=download_page_payload)
          print( " SUCCESS   : Login into freddiemac.embs.com succesful ")
          create_directory(DIR_NAME)
          create_directory(UNZIPPED_DIR_NAME)
          START_YEAR = get_start_year()

          print(" SUCCESS   : Threads creation started")
          threadspool=[]
          while END_YEAR >= START_YEAR :
              newThread=(Thread(target=get_data_from_url, args=(START_YEAR,)))
              threadspool.append(newThread)
              START_YEAR += 1

          for eachThread in threadspool:
              eachThread.start()

          for eachThread in threadspool:
              eachThread.join()
start_execution()
