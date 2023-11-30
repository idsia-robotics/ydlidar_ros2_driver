/*
 *  YDLIDAR SYSTEM
 *  YDLIDAR ROS 2 Node
 *
 *  Copyright 2017 - 2020 EAI TEAM
 *  http://www.eaibot.com
 *
 *  Largely modified by Jerome Guzzi, IDSIA, 2023
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

#include "core/math/angles.h"
#include "laser_geometry/laser_geometry.hpp"
#include "rcl_interfaces/msg/parameter_event.hpp"
#include "rclcpp/clock.hpp"
#include "rclcpp/rclcpp.hpp"
#include "rclcpp/time_source.hpp"
#include "sensor_msgs/msg/laser_scan.hpp"
#include "sensor_msgs/msg/point_cloud2.hpp"
#include "src/CYdLidar.h"
#include "tf2/exceptions.h"
#include "tf2_ros/async_buffer_interface.h"
#include "tf2_ros/buffer.h"
#include "tf2_ros/create_timer_ros.h"
#include "tf2_ros/transform_listener.h"
#include "tf2_sensor_msgs/tf2_sensor_msgs.hpp"

#if __has_include("rclcpp/event_handler.hpp")
#include "rclcpp/event_handler.hpp"
#define EVENT_HANDLER_AVAILABLE true
#else
#define EVENT_HANDLER_AVAILABLE false
#endif

using namespace std::chrono_literals;

namespace ymath = ydlidar::core::math;

#define ROS2Verision "1.0.1"

template <typename T>
struct LidarParameter {
  bool expose;
  bool dynamic;
  int id;
  T default_value;
};

const std::map<std::string, LidarParameter<std::string>> string_params{
    {"port", {true, false, LidarPropSerialPort, "/dev/ydlidar"}},
    {"ignore_array", {false, false, LidarPropIgnoreArray, ""}},
};

const std::map<std::string, LidarParameter<int>> int_params{
    {"baudrate", {true, false, LidarPropSerialBaudrate, 230400}},
    {"lidar_type", {true, false, LidarPropLidarType, TYPE_TRIANGLE}},
    {"device_type", {true, false, LidarPropDeviceType, YDLIDAR_TYPE_SERIAL}},
    {"sample_rate", {true, false, LidarPropSampleRate, 9}},
    {"abnormal_check_count", {true, false, LidarPropAbnormalCheckCount, 4}},
    {"intensity_bit", {true, false, LidarPropIntenstiyBit, 8}},
};

const std::map<std::string, LidarParameter<bool>> bool_params{
    {"fixed_resolution", {true, false, LidarPropFixedResolution, false}},
    {"reversion", {false, false, LidarPropReversion, false}},
    {"inverted", {false, false, LidarPropInverted, false}},
    {"auto_reconnect", {true, false, LidarPropAutoReconnect, true}},
    {"isSingleChannel", {true, false, LidarPropSingleChannel, false}},
    {"intensity", {true, false, LidarPropIntenstiy, false}},
    {"support_motor_dtr", {true, false, LidarPropSupportMotorDtrCtrl, false}},
};

const std::map<std::string, LidarParameter<float>> float_params{
    {"angle_max", {true, false, LidarPropMaxAngle, 180.0f}},
    {"angle_min", {true, false, LidarPropMinAngle, -180.0f}},
    {"range_max", {true, false, LidarPropMaxRange, 64.f}},
    {"range_min", {true, false, LidarPropMinRange, 0.1f}},
    {"frequency", {true, true, LidarPropScanFrequency, 10.f}},
};

enum class ActiveMode : int { OFF = 0, ON = 1, ON_DEMAND = 2 };

enum class Interpolation : int {
  none = 0,
  nearest_neighbor = 1,
  linear = 2,
};

template <typename T>
class DynamicPublisher {
  using Callback = std::function<void()>;

  ActiveMode activation;
  int number_of_subscribers;
  bool enabled;
  typename rclcpp::Publisher<T>::SharedPtr pub;
  Callback cb;
  rclcpp::Node *node;
  std::string _topic;
  rclcpp::TimerBase::SharedPtr count_subs_timer;

 public:
  explicit DynamicPublisher(rclcpp::Node *node)
      : activation(ActiveMode::OFF),
        number_of_subscribers(0),
        enabled(false),
        node(node){};

  void init(const std::string &topic, const rclcpp::QoS &qos, Callback callback,
            [[maybe_unused]] float count_subs_period_s = 1.0f) {
    enabled = true;
    cb = callback;
    rclcpp::PublisherOptions pub_options;
    _topic = topic;
#if EVENT_HANDLER_AVAILABLE
    pub_options.event_callbacks.matched_callback =
        [this](const rclcpp::MatchedInfo &s) {
          RCLCPP_DEBUG(node->get_logger(), "matched_callback %lu",
                       s.current_count);
          number_of_subscribers = s.current_count;
          cb();
        };
#endif  // EVENT_HANDLER_AVAILABLE
    pub = node->create_publisher<T>(topic, qos, pub_options);
#if !EVENT_HANDLER_AVAILABLE
    if (count_subs_period_s > 0) {
      count_subs_timer = node->create_wall_timer(
          std::chrono::milliseconds(
              static_cast<unsigned long>(1000 * count_subs_period_s)),
          [this]() {
            set_subscribers(node->count_subscribers(_topic));
            cb();
          });
    } else {
      RCLCPP_WARN(node->get_logger(),
                  "Will not monitor subscribers: negative period %.2f s",
                  count_subs_period_s);
    }
#endif  // EVENT_HANDLER_AVAILABLE
  }

  bool is_enabled() const { return enabled; }

  bool is_active() const {
    return activation == ActiveMode::ON ||
           (activation == ActiveMode::ON_DEMAND && number_of_subscribers > 0);
  }

  void set_activation(int v) {
    if (v < 0 || v > 2) return;
    ActiveMode value{v};
    if (activation != value) {
      activation = value;
      cb();
    }
  }

  void set_subscribers(int value) { number_of_subscribers = value; }

  void publish(const T &msg) { pub->publish(msg); }
};

static void set_msg_index(sensor_msgs::msg::LaserScan &msg, int index,
                          bool valid, float range, float intensity,
                          float max_range) {
  if (index < 0 || index >= msg.ranges.size()) return;
  if (!valid) {
    intensity = 0;
    range = 0;
  }
  msg.intensities[index] = intensity;
  if (intensity && range && range <= max_range) {
    msg.ranges[index] = range;
  } else {
    msg.ranges[index] = std::numeric_limits<float>::infinity();
  }
}

void fill_stamp_and_limits(sensor_msgs::msg::LaserScan &msg,
                           const LaserScan &scan) {
  msg.header.stamp.sec = RCL_NS_TO_S(scan.stamp);
  msg.header.stamp.nanosec = scan.stamp - RCL_S_TO_NS(msg.header.stamp.sec);
  msg.scan_time = scan.config.scan_time;
  msg.time_increment = scan.config.time_increment;
  msg.range_min = scan.config.min_range;
  msg.range_max = scan.config.max_range;
}

class YDLidarDriver : public rclcpp::Node {
 public:
  YDLidarDriver()
      : rclcpp::Node("ydlidar_driver"),
        laser(),
        scan_pub(this),
        pointcloud_pub(this),
        initialized(false),
        has_mask(false),
        negative_mask(),
        negative_mask_indices(),
        interpolation(Interpolation::nearest_neighbor),
        fixed_sector(true),
        tf_timeout(0, 100000000) {
    init_lidar_parameters();
    auto param_desc = rcl_interfaces::msg::ParameterDescriptor{};
    param_desc.read_only = true;

    long tf_timeout_ms =
        std::max(0L, declare_parameter("pointcloud.tf_timeout_ms", 100, param_desc));
    tf_timeout = rclcpp::Duration(0, 1000000 * tf_timeout_ms);

    int scan_mode = declare_parameter("scan.enable", 2);
    int pointcloud_mode = declare_parameter("pointcloud.enable", 2);
    add_intensity_to_pointcloud =
        declare_parameter("pointcloud.intensity", false, param_desc);
    set_interpolation(declare_parameter("interpolation", "linear"));
    fixed_sector = declare_parameter("fixed_sector", true);
    scan_msg.header.frame_id =
        declare_parameter("frame_id", "laser_frame", param_desc);
    bool use_sensor_data_qos =
        declare_parameter("use_sensor_data_qos", true, param_desc);
    float count_subs_period_s =
        declare_parameter("count_subs_period", 1.0f, param_desc);
    direction = declare_parameter("direction", 1, param_desc);
    delta_angle =
        ymath::from_degrees(declare_parameter("delta_angle", 0.0, param_desc));
    set_mask(declare_parameter("mask", ""));
    pointcloud_fixed_frame_id =
        declare_parameter("pointcloud.fixed_frame_id", "base_link", param_desc);
    pointcloud_frame_id =
        declare_parameter("pointcloud.frame_id", "base_link", param_desc);
    if (scan_mode < 0 && pointcloud_mode < 0) {
      RCLCPP_ERROR(get_logger(), "Enable at least one publisher");
      return;
    }
    const rclcpp::QoS qos =
        use_sensor_data_qos ? rclcpp::SensorDataQoS() : rclcpp::QoS{1};
    auto cb = std::bind(&YDLidarDriver::check_activation, this);
    scan_pub.init("scan", qos, cb, count_subs_period_s);
    scan_pub.set_activation(scan_mode);
    pointcloud_pub.init("pointcloud", qos, cb, count_subs_period_s);
    pointcloud_pub.set_activation(pointcloud_mode);
    tf_buffer_ = std::make_unique<tf2_ros::Buffer>(this->get_clock());
    auto timer_interface = std::make_shared<tf2_ros::CreateTimerROS>(
        this->get_node_base_interface(), this->get_node_timers_interface());
    tf_buffer_->setCreateTimerInterface(timer_interface);
    tf_listener_ = std::make_shared<tf2_ros::TransformListener>(*tf_buffer_);
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
  DynamicPublisher<sensor_msgs::msg::LaserScan> scan_pub;
  DynamicPublisher<sensor_msgs::msg::PointCloud2> pointcloud_pub;
  sensor_msgs::msg::LaserScan scan_msg;
  bool is_scanning;
  bool initialized;
  rclcpp::TimerBase::SharedPtr timer_;
  OnSetParametersCallbackHandle::SharedPtr callback_handle_;
  bool has_mask;
  std::vector<double> negative_mask;
  std::vector<bool> negative_mask_indices;
  int direction;
  float delta_angle;
  laser_geometry::LaserProjection projector;
  std::shared_ptr<tf2_ros::TransformListener> tf_listener_{nullptr};
  std::unique_ptr<tf2_ros::Buffer> tf_buffer_;
  std::string pointcloud_fixed_frame_id;
  std::string pointcloud_frame_id;
  Interpolation interpolation;
  bool fixed_sector;
  bool negative_mask_indices_valid;
  bool add_intensity_to_pointcloud;
  rclcpp::Duration tf_timeout;

  rcl_interfaces::msg::SetParametersResult parametersCallback(
      const std::vector<rclcpp::Parameter> &parameters) {
    rcl_interfaces::msg::SetParametersResult result;
    result.successful = true;
    for (const auto &parameter : parameters) {
      if (scan_pub.is_enabled() && parameter.get_name() == "scan.enable" &&
          parameter.get_type() == rclcpp::ParameterType::PARAMETER_INTEGER) {
        scan_pub.set_activation(parameter.as_int());
      }
      if (pointcloud_pub.is_enabled() &&
          parameter.get_name() == "pointcloud.enable" &&
          parameter.get_type() == rclcpp::ParameterType::PARAMETER_INTEGER) {
        pointcloud_pub.set_activation(parameter.as_int());
      }
      if (parameter.get_name() == "frequency" &&
          parameter.get_type() == rclcpp::ParameterType::PARAMETER_DOUBLE) {
        set_frequency(parameter.as_double());
      }
      if (parameter.get_name() == "interpolation" &&
          parameter.get_type() == rclcpp::ParameterType::PARAMETER_STRING) {
        set_interpolation(parameter.as_string());
      }
      if (parameter.get_name() == "fixed_sector" &&
          parameter.get_type() == rclcpp::ParameterType::PARAMETER_BOOL) {
        fixed_sector = parameter.as_bool();
      }
      if (parameter.get_name() == "mask" &&
          parameter.get_type() == rclcpp::ParameterType::PARAMETER_STRING) {
        set_mask(parameter.as_string());
      }
    }
    return result;
  }

  void init_lidar_parameters() {
    auto param_desc = rcl_interfaces::msg::ParameterDescriptor{};
    for (auto &[k, v] : string_params) {
      param_desc.read_only = !v.dynamic;
      std::string value =
          v.expose ? declare_parameter(k, v.default_value, param_desc)
                   : v.default_value;
      laser.setlidaropt(v.id, value.c_str(), value.size());
    }
    for (auto &[k, v] : int_params) {
      param_desc.read_only = !v.dynamic;
      int value = v.expose ? declare_parameter(k, v.default_value, param_desc)
                           : v.default_value;
      laser.setlidaropt(v.id, &value, sizeof(int));
    }
    for (auto &[k, v] : bool_params) {
      param_desc.read_only = !v.dynamic;
      bool value = v.expose ? declare_parameter(k, v.default_value, param_desc)
                            : v.default_value;
      laser.setlidaropt(v.id, &value, sizeof(bool));
    }
    for (auto &[k, v] : float_params) {
      param_desc.read_only = !v.dynamic;
      float value = v.expose ? declare_parameter(k, v.default_value, param_desc)
                             : v.default_value;
      laser.setlidaropt(v.id, &value, sizeof(float));
    }
  }

  void set_mask(const std::string &value) {
    std::string s;
    std::istringstream f(value);
    bool valid = true;
    negative_mask.clear();
    while (std::getline(f, s, ',')) {
      try {
        const float angle = ymath::from_degrees(std::stof(s));
        negative_mask.push_back(angle);
      } catch (std::exception &e) {
        valid = false;
        RCLCPP_WARN(
            get_logger(),
            "Failed parsing mask %s ... should have format <float>,<float>,...",
            value.c_str());
        break;
      }
    }
    if (!valid) {
      negative_mask.clear();
    }
    if (negative_mask.size() % 2) {
      negative_mask.pop_back();
    }
    has_mask = valid && negative_mask.size() > 1;
    negative_mask_indices_valid = false;
    for (size_t i = 0; i < negative_mask.size(); i += 2) {
      RCLCPP_INFO(get_logger(), "Will filter out angles in [%.2f, %.2f]",
                  negative_mask[i], negative_mask[i] + negative_mask[i + 1]);
    }
    if (!has_mask) {
      RCLCPP_INFO(get_logger(), "No angular filter");
    }
  }

  void fill_sector(sensor_msgs::msg::LaserScan &msg, const LaserScan &scan,
                   size_t desired_size) const {
    size_t size = scan.points.size();
    float start_angle =
        ymath::normalize_angle(transform_angle(scan.points.at(0).angle));
    const float end_angle = transform_angle(scan.points.at(size - 1).angle);
    float da = ymath::normalize_angle(end_angle - start_angle);
    if (direction > 0 && da < M_PI) {
      da += 2 * M_PI;
    } else if (direction < 0 && da > -M_PI) {
      da -= 2 * M_PI;
    }
    if (direction > 0 && start_angle > 0) {
      start_angle -= 2 * M_PI;
    } else if (direction < 0 && start_angle < 0) {
      start_angle += 2 * M_PI;
    }
    msg.angle_min = start_angle;
    msg.angle_increment = da / (size - 1);
    msg.angle_max = start_angle + msg.angle_increment * (desired_size - 1);
  }

  void fill_interpolated_range_and_intensity(sensor_msgs::msg::LaserScan &msg,
                                             const LaserScan &scan,
                                             bool linear) {
    size_t j = 0;
    float angle = r_transform_angle(msg.angle_min);
    const size_t scan_size = scan.points.size();
    const float max_d = abs(msg.angle_increment) * msg.range_max * 1.1f;
    for (size_t i = 0; i < msg.ranges.size();
         i++, angle += direction * msg.angle_increment) {
      float range;
      float intensity;
      if (j < scan_size - 1) {
        float d0 = ymath::normalize_angle(scan.points[j].angle - angle);
        if (d0 > 0) {
          range = scan.points[j].range;
          intensity = scan.points[j].intensity;
        } else {
          float d1;
          j++;
          for (; j < scan_size; ++j) {
            d1 = ymath::normalize_angle(scan.points[j].angle - angle);
            if (d1 > 0) {
              break;
            }
            d0 = d1;
          }
          const float r0 = scan.points[j - 1].range;
          const float i0 = scan.points[j - 1].intensity;
          const float r1 = scan.points[j].range;
          const float i1 = scan.points[j].intensity;
          if (linear && r0 && r1 && abs(r0 - r1) < max_d) {
            const float f = d0 / (d0 - d1);
            range = r0 + (r1 - r0) * f;
            intensity = i0 + (i1 - i0) * f;
          } else {
            if (abs(d0) < abs(d1)) {
              range = r0;
              intensity = i0;
            } else {
              range = r1;
              intensity = i1;
            }
          }
        }
      } else {
        range = scan.points[j].range;
        intensity = scan.points[j].intensity;
      }
      set_msg_index(msg, i, !masked_index(i), range, intensity,
                    scan.config.max_range);
    }
  }

  void set_interpolation(const std::string &value) {
    if (value == "nearest") {
      interpolation = Interpolation::nearest_neighbor;
    } else if (value == "linear") {
      interpolation = Interpolation::linear;
    } else {
      interpolation = Interpolation::none;
    }
  }

  void publish_pointcloud() {

    if(!rclcpp::ok()) return;

    // See laser_geometry/src/laser_geometry.cpp
    // We need to wait until we can transform at the timepoint of the final ranging.
    rclcpp::Time end_time = scan_msg.header.stamp;
    if (!scan_msg.ranges.empty()) {
      end_time += rclcpp::Duration::from_seconds(
          static_cast<double>(scan_msg.ranges.size() - 1) *
          static_cast<double>(scan_msg.time_increment));
    }

    tf_buffer_->waitForTransform(
        pointcloud_fixed_frame_id, scan_msg.header.frame_id, end_time,
        tf_timeout,
        [this,
         scan_msg = scan_msg](const tf2_ros::TransformStampedFuture &future) {
          try {
            future.get();
            _publish_pointcloud(scan_msg);
          } catch (const tf2::TransformException &ex) {
            RCLCPP_WARN(get_logger(),
                        "Waited too long for transform between %s and %s",
                        pointcloud_fixed_frame_id.c_str(),
                        scan_msg.header.frame_id.c_str());
          }
        });
  }

  void _publish_pointcloud(const sensor_msgs::msg::LaserScan &scan_msg) {
    sensor_msgs::msg::PointCloud2 cloud_msg;
    try {
      projector.transformLaserScanToPointCloud(
          pointcloud_fixed_frame_id, scan_msg, cloud_msg, *tf_buffer_, -1.0,
          add_intensity_to_pointcloud
              ? laser_geometry::channel_option::Intensity
              : laser_geometry::channel_option::None);
    } catch (const tf2::TransformException &ex) {
      RCLCPP_WARN(get_logger(), ex.what());
      return;
    }
    if (pointcloud_frame_id != pointcloud_fixed_frame_id) {
      try {
        cloud_msg = tf_buffer_->transform(cloud_msg, pointcloud_frame_id);
      } catch (const tf2::TransformException &ex) {
        RCLCPP_WARN(get_logger(), ex.what());
        return;
      }
    }
    pointcloud_pub.publish(cloud_msg);
  }

  void update_mask_indices(const sensor_msgs::msg::LaserScan &msg) {
    if (!has_mask) return;
    negative_mask_indices.resize(msg.ranges.size());
    float angle = msg.angle_min;
    for (size_t i = 0; i < msg.ranges.size();
         i++, angle += msg.angle_increment) {
      negative_mask_indices[i] = masked(angle);
    }
    negative_mask_indices_valid = true;
  }

  bool masked_index(int index) const {
    return has_mask && negative_mask_indices[index];
  }

  bool masked(float angle) const {
    if (!has_mask) return false;
    for (size_t j = 0; j < negative_mask.size(); j += 2) {
      const float da = ymath::normalize_angle(angle - negative_mask[j]);
      if (da > 0 && da < negative_mask[j + 1]) {
        return true;
      }
    }
    return false;
  }

  void update_scan_msg(const LaserScan &scan,
                       sensor_msgs::msg::LaserScan &msg) {
    fill_stamp_and_limits(msg, scan);
    size_t size = laser.get_fixed_size();

    if (!fixed_sector || size != msg.ranges.size()) {
      if (!fixed_sector) {
        size = scan.points.size();
      }
      fill_sector(msg, scan, size);
      msg.ranges.resize(size);
      msg.intensities.resize(size);
      negative_mask_indices_valid = false;
    }

    if (interpolation == Interpolation::none) {
      size = std::min(scan.points.size(), msg.ranges.size());
      for (size_t i = 0; i < size; i++) {
        set_msg_index(msg, i, !masked(transform_angle(scan.points[i].angle)),
                      scan.points[i].range, scan.points[i].intensity,
                      scan.config.max_range);
      }
    } else {
      if (!negative_mask_indices_valid) {
        update_mask_indices(msg);
      }
      fill_interpolated_range_and_intensity(
          msg, scan, interpolation == Interpolation::linear);
    }
  }

  float transform_angle(float value) const {
    return direction * value + delta_angle;
  }

  float r_transform_angle(float value) const {
    return direction * (value - delta_angle);
  }

  bool update() {
    if (!ydlidar::os_isOk()) return false;
    if (!is_scanning) return true;
    LaserScan scan;
    if (laser.doProcessSimple(scan)) {
      if (scan_pub.is_active() || pointcloud_pub.is_active()) {
        update_scan_msg(scan, scan_msg);
      }
      if (scan_pub.is_active()) {
        scan_pub.publish(scan_msg);
      }
      if (pointcloud_pub.is_active()) {
        publish_pointcloud();
      }
      return true;
    }
    return false;
  }

  bool should_be_active() const {
    return scan_pub.is_active() || pointcloud_pub.is_active();
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
