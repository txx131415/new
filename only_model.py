#!/usr/bin/env python

import requests
import time
import threading
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


class Cube(object):

    def __init__(self, base_url):
        self.base_url = base_url
        self.headers = {
            'content-type': "application/json",
            'authorization': "Basic QURNSU46S1lMSU4=",
            'cache-control': "no-cache",
            'accept': "application/vnd.apache.kylin-v2+json"
        }

    def buildIncreCube(self, start_time, end_time):
        incre_cube_url = self.base_url + "/models/segments"
        payload = '{"start": %s, "end": %s, "model":"test_script", "project": "hand_script_nazhai"}' % (
        start_time, end_time)
        response = requests.request("POST", incre_cube_url, data=payload, headers=self.headers, verify=False)
        return_code = response.status_code
        logger.info("Segment range: {start} - {end}, status_code: {status}".format(start=start_time, end=end_time,
                                                                                   status=return_code))
        print(return_code)


def fileLogger(log_file):
    handler = logging.FileHandler(log_file)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def main():
    log_file = "buildincrecube.log"
    fileLogger(log_file)

    base_url = "http://10.1.2.18:7070/kylin/api"

    start_time = 0
    segment_nums = 3

    build_interval = 10
    ssb = Cube(base_url)
    startTimeList = []

    for i in range(segment_nums):
        startTimeList.append(start_time)
        start_time = start_time + 24 * 3600 * 1000
    try:
        while True:
            for i in range(len(startTimeList)):
                start_time = startTimeList[i]
                end_time = start_time + 24 * 3600 * 1000
                return_code = ssb.buildIncreCube(start_time, end_time)
                if return_code == 200:
                    startTimeList.remove(start_time)
                if len(startTimeList) < 10:
                    start_time = end_time
                    startTimeList.append(start_time)
            logger.info("-----------------length of startTimeList: " + str(len(startTimeList)))
            time.sleep(build_interval)
    except (KeyboardInterrupt, EOFError):
        logger.info('interrupt by Ctrl+C or Ctrl+D')


if __name__ == '__main__':
    main()
