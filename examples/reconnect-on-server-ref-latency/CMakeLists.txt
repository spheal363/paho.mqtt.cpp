# CMakeLists.txt for reconnect-on-server-ref-latency example
#
# This CMake file builds the reconnect-on-server-ref-latency example applications.
#
#*******************************************************************************
# This is part of the Paho MQTT C++ client library.
#
# Copyright (c) 2016-2024
# 
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v2.0
# and Eclipse Distribution License v1.0 which accompany this distribution.
# 
# The Eclipse Public License is available at
#   http://www.eclipse.org/legal/epl-v20.html
# and the Eclipse Distribution License is available at
#   http://www.eclipse.org/org/documents/edl-v10.php.
# 
# Contributors:
#   Initial implementation
#*******************************************************************************/

# Build the reconnect_on_server_ref_latency executable
# add_executable(reconnect_on_server_ref_latency 
#     reconnect_on_server_ref_latency.cpp
# )

# Build the reconnect_on_server_ref_latency_test executable
add_executable(reconnect_on_server_ref_latency_test 
    reconnect_on_server_ref_latency_test.cpp
)

# Link libraries for executables
target_link_libraries(reconnect_on_server_ref_latency_test PahoMqttCpp::paho-mqttpp3)

# Set C++17 standard for executables
set_target_properties(reconnect_on_server_ref_latency_test PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)

# Add PAHO_MQTTPP_IMPORTS definition if building shared library
if(PAHO_BUILD_SHARED)
    target_compile_definitions(reconnect_on_server_ref_latency_test PRIVATE PAHO_MQTTPP_IMPORTS)
endif()

# Install targets
include(GNUInstallDirs)
install(TARGETS reconnect_on_server_ref_latency_test
    EXPORT PahoMqttCppSamples
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

