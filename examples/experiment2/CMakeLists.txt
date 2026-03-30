# CMakeLists.txt for experiment2 load test publisher
#
# This CMake file builds the load test publisher application.
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

# Build the load_test_publisher executable
add_executable(load_test_publisher 
    load_test_publisher.cpp
)

# Link libraries for executables
target_link_libraries(load_test_publisher PahoMqttCpp::paho-mqttpp3)

# Set C++17 standard for executables
set_target_properties(load_test_publisher PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)

# Add PAHO_MQTTPP_IMPORTS definition if building shared library
if(PAHO_BUILD_SHARED)
    target_compile_definitions(load_test_publisher PRIVATE PAHO_MQTTPP_IMPORTS)
endif()

# Install targets
include(GNUInstallDirs)
install(TARGETS load_test_publisher
    EXPORT PahoMqttCppSamples
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
