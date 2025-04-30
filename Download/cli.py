import os
import sys
import extraction

import funding

if __name__ == '__main__':

     command = os.getenv('ACTION')
     if command == 'download':
          funding.download_all_data()
     elif command == 'analyze':
          extraction.regenerate_all_files()

