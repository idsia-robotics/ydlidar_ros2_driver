import os
from typing import List

import xacro
from ament_index_python.packages import get_package_share_directory
from launch_ros.actions import Node

import launch.actions
from launch import LaunchContext, LaunchDescription
from launch.utilities import perform_substitutions

args_descriptions = {
    "name": "Name used as a tf prefix and namespace",
    "model": "Lidar model",
    "base_frame_id": "The base frame",
    "lidar_frame_id": "The lidar frame",
}

def urdf(name: str = '', model: str = '', base_frame_id: str = "base_link",
         lidar_frame_id: str = "base_laser") -> str:
    urdf_xacro = os.path.join(
        get_package_share_directory('ydlidar_ros2_driver'), 'urdf',
        f'{model}.urdf.xacro')
    xacro_keys = [
        k for k, _ in urdf.__annotations__.items()
        if k not in ('return', 'model')
    ]
    kwargs = dict(locals())
    xacro_args = [
        f'{arg_name}:={kwargs.get(arg_name)}' for arg_name in xacro_keys
    ]
    opts, input_file_name = xacro.process_args([urdf_xacro] + xacro_args)
    try:
        doc = xacro.process_file(input_file_name, **vars(opts))
        return doc.toprettyxml(indent='  ')
    except Exception as e:
        print(e)
        return ''


def robot_state_publisher(
        context: LaunchContext,
        **substitutions: launch.substitutions.LaunchConfiguration
) -> List[Node]:
    kwargs = {
        k: perform_substitutions(context, [v])
        for k, v in substitutions.items()
    }
    params = {'robot_description': urdf(**kwargs), 'publish_frequency': 100.0}
    node = Node(package='robot_state_publisher',
                executable='robot_state_publisher',
                name='robot_state_publisher',
                parameters=[params],
                output='screen',
                arguments=["--ros-args", "--log-level", "warn"])
    return [node]


def generate_launch_description() -> None:
    arguments = [
        launch.actions.DeclareLaunchArgument(
            k,
            default_value=str(urdf.__defaults__[i]),
            description=args_descriptions.get(k, ''))
        for i, (k, _) in enumerate(urdf.__annotations__.items())
        if k != 'return'
    ]
    kwargs = {
        k: launch.substitutions.LaunchConfiguration(k)
        for (k, _) in urdf.__annotations__.items() if k != 'return'
    }
    return LaunchDescription(arguments + [
        launch.actions.OpaqueFunction(function=robot_state_publisher,
                                      kwargs=kwargs),
    ])
