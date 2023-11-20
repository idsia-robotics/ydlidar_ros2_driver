/*
 *  YDLIDAR SYSTEM
 *  YDLIDAR ROS 2 Node
 *
 *  Copyright 2017 - 2020 EAI TEAM
 *  http://www.eaibot.com
 *
 */

#ifdef _MSC_VER
#ifndef _USE_MATH_DEFINES
#define _USE_MATH_DEFINES
#endif
#endif

#include <math.h>

#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "rcl_interfaces/msg/parameter_event.hpp"
#include "rclcpp/clock.hpp"
#include "rclcpp/rclcpp.hpp"
#include "rclcpp/time_source.hpp"
#include "sensor_msgs/msg/laser_scan.hpp"
#include "src/CYdLidar.h"

#if __has_include("rclcpp/event_handler.hpp")
#include "rclcpp/event_handler.hpp"
#define EVENT_HANDLER_AVAILABLE true
#else
#define EVENT_HANDLER_AVAILABLE false
#endif

using namespace std::chrono_literals;

#define ROS2Verision "1.0.1"

template <typename T>
struct LidarParameter {
  bool dynamic;
  int id;
  T default_value;
};

const std::map<std::string, LidarParameter<std::string>> string_params{
    {"port", {false, LidarPropSerialPort, "/dev/ydlidar"}},
    {"ignore_array", {false, LidarPropIgnoreArray, ""}},
};

const std::map<std::string, LidarParameter<int>> int_params{
    {"baudrate", {false, LidarPropSerialBaudrate, 230400}},
    {"lidar_type", {false, LidarPropLidarType, TYPE_TRIANGLE}},
    {"device_type", {false, LidarPropDeviceType, YDLIDAR_TYPE_SERIAL}},
    {"sample_rate", {false, LidarPropSampleRate, 9}},
    {"abnormal_check_count", {false, LidarPropAbnormalCheckCount, 4}},
    {"intensity_bit", {false, LidarPropIntenstiyBit, 8}},
};

const std::map<std::string, LidarParameter<bool>> bool_params{
    {"fixed_resolution", {false, LidarPropFixedResolution, false}},
    {"reversion", {false, LidarPropReversion, true}},
    {"inverted", {false, LidarPropInverted, true}},
    {"auto_reconnect", {false, LidarPropAutoReconnect, true}},
    {"isSingleChannel", {false, LidarPropSingleChannel, false}},
    {"intensity", {false, LidarPropIntenstiy, false}},
    {"support_motor_dtr", {false, LidarPropSupportMotorDtrCtrl, false}},
};

const std::map<std::string, LidarParameter<float>> float_params{
    {"angle_max", {false, LidarPropMaxAngle, 180.0f}},
    {"angle_min", {false, LidarPropMinAngle, -180.0f}},
    {"range_max", {false, LidarPropMaxRange, 64.f}},
    {"range_min", {false, LidarPropMinRange, 0.1f}},
    {"frequency", {false, LidarPropScanFrequency, 10.f}},
};

enum class ActiveMode : int { OFF = 0, ON = 1, ON_DEMAND = 2 };

class YDLidarDriver : public rclcpp::Node {
 public:
  YDLidarDriver()
      : rclcpp::Node("ydlidar_driver"),
        laser(),
        laser_pub(nullptr),
        msg(),
        subscribers(0),
        activation(ActiveMode::OFF),
        initialized(false) {
    init_lidar_parameters();
    msg.header.frame_id = declare_parameter("frame_id", "laser_frame");
    bool use_sensor_data_qos = declare_parameter("use_sensor_data_qos", true);
    float count_subs_period_s = declare_parameter("count_subs_period", 1.0f);
    rclcpp::PublisherOptions pub_options;
    const std::string topic = "scan";
#if EVENT_HANDLER_AVAILABLE
    RCLCPP_INFO(get_logger(), "ON DEMAND AVAILABLE");
    pub_options.event_callbacks.matched_callback =
        [this](const rclcpp::MatchedInfo &s) {
          RCLCPP_INFO(get_logger(), "matched_callback %d", s.current_count);
          subscribers = s.current_count;
          check_activation();
        };
#else
    if (count_subs_period_s > 0) {
      count_subs_timer = create_wall_timer(
          std::chrono::milliseconds(
              static_cast<unsigned long>(1000 * count_subs_period_s)),
          [this, topic]() {
            subscribers = count_subscribers(topic);
            check_activation();
          });
    } else {
      RCLCPP_WARN(get_logger(),
                  "Will not monitor subscribers: negative period %.2f s",
                  count_subs_period_s);
    }
#endif  // EVENT_HANDLER_AVAILABLE
    laser_pub = create_publisher<sensor_msgs::msg::LaserScan>(
        topic, use_sensor_data_qos ? rclcpp::SensorDataQoS() : rclcpp::QoS{1},
        pub_options);
    set_activation(declare_parameter("activation", 2));
    callback_handle_ = add_on_set_parameters_callback(std::bind(
        &YDLidarDriver::parametersCallback, this, std::placeholders::_1));
    timer_ = create_wall_timer(20ms, std::bind(&YDLidarDriver::update, this));
  }

  bool init() {
    bool ret = laser.initialize();
    if (ret) {
      initialized = true;
      check_activation();
      return true;
    }
    RCLCPP_ERROR(get_logger(), "%s\n", laser.DescribeError());
    return false;
  }

  void deinit() {
    if (initialized) {
      is_scanning = false;
      initialized = false;
      laser.turnOff();
      laser.disconnecting();
    }
  }

 private:
  CYdLidar laser;
  rclcpp::Publisher<sensor_msgs::msg::LaserScan>::SharedPtr laser_pub;
  sensor_msgs::msg::LaserScan msg;
  bool is_scanning;
  ActiveMode activation;
  int subscribers;
  bool initialized;
  rclcpp::TimerBase::SharedPtr timer_;
  OnSetParametersCallbackHandle::SharedPtr callback_handle_;
  rclcpp::TimerBase::SharedPtr count_subs_timer;

  rcl_interfaces::msg::SetParametersResult parametersCallback(
      const std::vector<rclcpp::Parameter> &parameters) {
    rcl_interfaces::msg::SetParametersResult result;
    result.successful = true;
    for (const auto &parameter : parameters) {
      if (parameter.get_name() == "activation" &&
          parameter.get_type() == rclcpp::ParameterType::PARAMETER_INTEGER) {
        set_activation(parameter.as_int());
      }
      if (parameter.get_name() == "frequency" &&
          parameter.get_type() == rclcpp::ParameterType::PARAMETER_DOUBLE) {
        set_frequency(parameter.as_double());
      }
    }
    return result;
  }

  void init_lidar_parameters() {
    for (auto &[k, v] : string_params) {
      std::string value = declare_parameter(k, v.default_value);
      laser.setlidaropt(v.id, value.c_str(), value.size());
    }
    for (auto &[k, v] : int_params) {
      int value = declare_parameter(k, v.default_value);
      laser.setlidaropt(v.id, &value, sizeof(int));
    }
    for (auto &[k, v] : bool_params) {
      bool value = declare_parameter(k, v.default_value);
      laser.setlidaropt(v.id, &value, sizeof(bool));
    }
    for (auto &[k, v] : float_params) {
      float value = declare_parameter(k, v.default_value);
      laser.setlidaropt(v.id, &value, sizeof(float));
    }
  }

  bool update() {
    if (!ydlidar::os_isOk()) return false;
    if (!is_scanning) return true;
    LaserScan scan;
    if (laser.doProcessSimple(scan)) {
      msg.header.stamp.sec = RCL_NS_TO_S(scan.stamp);
      msg.header.stamp.nanosec = scan.stamp - RCL_S_TO_NS(msg.header.stamp.sec);
      msg.angle_min = scan.config.min_angle;
      msg.angle_max = scan.config.max_angle;
      msg.angle_increment = scan.config.angle_increment;
      msg.scan_time = scan.config.scan_time;
      msg.time_increment = scan.config.time_increment;
      msg.range_min = scan.config.min_range;
      msg.range_max = scan.config.max_range;
      int size = (scan.config.max_angle - scan.config.min_angle) /
                     scan.config.angle_increment +
                 1;
      msg.ranges.resize(size);
      msg.intensities.resize(size);
      for (size_t i = 0; i < scan.points.size(); i++) {
        int index = std::ceil((scan.points[i].angle - scan.config.min_angle) /
                              scan.config.angle_increment);
        if (index >= 0 && index < size) {
          if (scan.points[i].range >= scan.config.min_range) {
            msg.ranges[index] = scan.points[i].range;
            msg.intensities[index] = scan.points[i].intensity;
          } else {
            msg.ranges[index] = 0;
            msg.intensities[index] = 0;
          }
        }
      }
      laser_pub->publish(msg);
      return true;
    }
    return false;
  }

  bool should_be_active() const {
    return activation == ActiveMode::ON ||
           (activation == ActiveMode::ON_DEMAND && subscribers > 0);
  }

  void check_activation() {
    if (!initialized) return;
    bool desired = should_be_active();
    if (desired != is_scanning) {
      if (desired) {
        if (laser.turnOn()) {
          is_scanning = true;
          RCLCPP_INFO(get_logger(), "started");
        }
      } else {
        laser.turnOff();
        is_scanning = false;
        RCLCPP_INFO(get_logger(), "stopped");
      }
    }
  }

  void set_activation(int v) {
    RCLCPP_WARN(get_logger(), "set_activation(%d)", v);
    if (v < 0 || v > 2) return;

    ActiveMode value{v};

    /*
        if ((value == ActiveMode::ON_DEMAND) && !ON_DEMAND_AVAILABLE) {
          RCLCPP_WARN(get_logger(), "On-demand only supported from ROS 2 Iron");
          return;
        }
    */
    if (activation != value) {
      activation = value;
      check_activation();
    }
  }

  void set_frequency(float value) {
    if (initialized) {
      float current = 0.0f;
      laser.getlidaropt(LidarPropScanFrequency, &current, sizeof(float));
      if (value != current) {
        if (is_scanning) {
          laser.turnOff();
        }
        laser.setlidaropt(LidarPropScanFrequency, &value, sizeof(float));
        laser.checkScanFrequency();
        if (is_scanning) {
          laser.turnOn();
        }
      }
    }
  }
};

int main(int argc, char *argv[]) {
  rclcpp::init(argc, argv);
  auto node = std::make_shared<YDLidarDriver>();
  if (node->init()) {
    rclcpp::spin(node);
    node->deinit();
  }
  rclcpp::shutdown();
  return 0;
}
