import time
import rospy
import json
import threading
import paho.mqtt.client as mqtt
from rospy_message_converter import json_message_converter, message_converter
from cai_msgs.msg import *
from cai_msgs.srv import *

class RosMqttBridge:
    def __init__(self, mqtt_broker, mqtt_port, rid='r2'):
        rospy.init_node("ros2mqtt_bridge_node", anonymous=True)

        if rid == None:
            self._rid = ''
        else:
            self._rid = rid

        self.mqtt_broker = mqtt_broker
        self.mqtt_port = mqtt_port
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.connect(self.mqtt_broker, self.mqtt_port, 60)

        self.subs, self.srvs = [],[]

        self.subs.append(rospy.Subscriber("%s/robot_state"%self._rid, RobotState, self.robotstate_cb))
        self.subs.append(rospy.Subscriber("%s/JobScheduler/feedback"%self._rid, JobFeedback, self.job_feedback_cb))
        self.srvs.append(rospy.Service("%s/JobScheduler/result"%self._rid, JobResult, self.job_result_cb))

        self.job_result_flag = False

    def on_connect(self, client, userdata, flags, rc):
        rospy.loginfo("[ROS-MQTT Bridge] Connected to MQTT broker!!")
        self.mqtt_client.subscribe(f"/{self._rid}/JobScheduler/result/response")
        self.mqtt_client.subscribe(f"/{self._rid}/JobScheduler/goal/request")    
        self.mqtt_client.subscribe(f"/{self._rid}/JobScheduler/cancel/request")
        self.mqtt_client.subscribe(f"/{self._rid}/JobScheduler/pause/request")
        self.mqtt_client.subscribe(f"/{self._rid}/JobScheduler/resume/request")

    def on_message(self, client, userdata, msg):
        mqtt_topic = msg.topic
        mqtt_message = msg.payload.decode("utf-8")
        
        if mqtt_topic == f"/{self._rid}/JobScheduler/result/response":
            self.job_result_from_broker = mqtt_message
            self.job_result_flag = True
            pass

        elif mqtt_topic == f"/{self._rid}/JobScheduler/goal/request":
            rospy.loginfo(f"Received JOB GOAL request from MQTT!!")
            rospy.wait_for_service("%s/JobScheduler/goal"%self._rid)
            self.job_goal_cnt = rospy.ServiceProxy("%s/JobScheduler/goal"%self._rid, JobGoal)
            ros_message = message_converter.convert_dictionary_to_ros_message("cai_msgs/JobGoal", json.loads(mqtt_message), kind="request", strict_mode="True")
            res = self.job_goal_cnt(ros_message)
            data = json_message_converter.convert_ros_message_to_json(res)
            self.mqtt_client.publish(f"/{self._rid}/JobScheduler/goal/response", data)
            rospy.loginfo(f"Sent JOB GOAL response to MQTT")
            pass

        elif mqtt_topic == f"/{self._rid}/JobScheduler/cancel/request":
            rospy.loginfo(f"Received JOB CANCEL request from MQTT!!")
            rospy.wait_for_service("%s/JobScheduler/cancel"%self._rid)
            self.job_cancel_cnt = rospy.ServiceProxy("%s/JobScheduler/cancel"%self._rid, JobCancel)
            ros_message = message_converter.convert_dictionary_to_ros_message("cai_msgs/JobCancel", json.loads(mqtt_message), kind="request", strict_mode="True")
            res = self.job_cancel_cnt(ros_message)
            data = json_message_converter.convert_ros_message_to_json(res)
            self.mqtt_client.publish(f"/{self._rid}/JobScheduler/cancel/response", data)
            rospy.loginfo(f"Sent JOB CANCEL response to MQTT!!")
            pass

        elif mqtt_topic == f"/{self._rid}/JobScheduler/pause/request":
            rospy.loginfo(f"Received JOB PAUSE request from MQTT!!")
            rospy.wait_for_service("%s/JobScheduler/pause"%self._rid)
            self.job_pause_cnt = rospy.ServiceProxy("%s/JobScheduler/pause"%self._rid, JobPause)
            ros_message = message_converter.convert_dictionary_to_ros_message("cai_msgs/JobPause", json.loads(mqtt_message), kind="request", strict_mode="True")
            res = self.job_pause_cnt(ros_message)
            data = json_message_converter.convert_ros_message_to_json(res)
            self.mqtt_client.publish(f"/{self._rid}/JobScheduler/pause/response", data)
            rospy.loginfo(f"Sent JOB PAUSE response to MQTT!!")
            pass

        elif mqtt_topic == f"/{self._rid}/JobScheduler/resume/request":
            rospy.loginfo(f"Received JOB RESUME request from MQTT!!")
            rospy.wait_for_service("%s/JobScheduler/resume"%self._rid)
            self.job_resume_cnt = rospy.ServiceProxy("%s/JobScheduler/resume"%self._rid, JobResume)
            ros_message = message_converter.convert_dictionary_to_ros_message("cai_msgs/JobResume", json.loads(mqtt_message), kind="request", strict_mode="True")
            res = self.job_resume_cnt(ros_message)
            data = json_message_converter.convert_ros_message_to_json(res)
            self.mqtt_client.publish(f"/{self._rid}/JobScheduler/resume/response", data)
            rospy.loginfo(f"Sent JOB RESUME response to MQTT!!")
            pass

    def robotstate_cb(self, msg):
        data = json_message_converter.convert_ros_message_to_json(msg)
        self.mqtt_client.publish(f"/{self._rid}/robot_state", data)
        rospy.loginfo(f"Published ROBOT STATE to MQTT!!")

    def job_feedback_cb(self, msg):
        data = json_message_converter.convert_ros_message_to_json(msg)
        self.mqtt_client.publish(f"/{self._rid}/JobScheduler/feedback", data)
        rospy.loginfo(f"Published JOB FEEDBACK to MQTT!!")

    def job_result_cb(self, msg):
        data = json_message_converter.convert_ros_message_to_json(msg)
        self.mqtt_client.publish(f"/{self._rid}/JobScheduler/result/request", data)
        rospy.loginfo(f"Called JOB RESULT request to MQTT!!")

        ## wait for subscribe job result response
        timecount = time.time()
        while not self.job_result_flag:
            if time.time()-timecount > 3.0:
                break
            pass
            rospy.sleep(0.1)

        if self.job_result_flag == True:
            res = message_converter.convert_dictionary_to_ros_message("cai_msgs/JobResult", json.loads(self.job_result_from_broker), kind="response", strict_mode="True")
            rospy.loginfo(f"Received JOB RESULT response from MQTT!!")
            self.job_result_flag = False
            return res

        else:
            raise Exception("Cannot get JOB RESULT..")

    def run(self):
        self.mqtt_client.connect(self.mqtt_broker, self.mqtt_port, 60)
        self.mqtt_client.loop_start()

        while not self.mqtt_client.is_connected():
            time.sleep(1)

        ros_thread = threading.Thread(target=rospy.spin)
        ros_thread.start()

if __name__ == '__main__':
    mqtt_broker = "192.168.0.169"
    mqtt_port = 1883
    
    bridge = RosMqttBridge(mqtt_broker, mqtt_port)
    bridge.run()