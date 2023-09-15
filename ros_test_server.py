import rospy
from cai_msgs.srv import *

def jobgoal_cb(req):
    res = JobGoalResponse()
    res.success = True
    res.error_code = 0
    rospy.loginfo("Respond JOB GOAL to ros bridge!!")
    return res

def jobcancel_cb(req):
    res = JobCancelResponse()
    res.success = False
    res.error_code = 1
    rospy.loginfo("Respond JOB CANCEL to ros bridge!!")
    return res

def jobpause_cb(req):
    res = JobPauseResponse()
    res.success = False
    res.error_code = 1
    rospy.loginfo("Respond JOB PAUSE to ros bridge!!")
    return res

def jobresume_cb(req):
    res = JobResumeResponse()
    res.success = True
    res.error_code = 0
    rospy.loginfo("Respond JOB RESUME to ros bridge!!")
    return res

def ros_server():
    rospy.init_node('ros_test_server')
    jobgoal_service = rospy.Service('r2/JobScheduler/goal', JobGoal, jobgoal_cb)
    jobcancel_service = rospy.Service('r2/JobScheduler/cancel', JobCancel, jobcancel_cb)
    jobpause_service = rospy.Service('r2/JobScheduler/pause', JobPause, jobpause_cb)
    jobresume_service = rospy.Service('r2/JobScheduler/resume', JobResume, jobresume_cb)
    rospy.loginfo("ROS Server Ready.")
    rospy.spin()

if __name__ == '__main__':
    ros_server()
