include(FetchContent)

FetchContent_Declare(
  sonic-cpp
  GIT_REPOSITORY https://github.com/bytedance/sonic-cpp.git
  GIT_TAG        v1.0.2
)

set(BUILD_UNITTEST OFF CACHE BOOL "Disable sonic-cpp unittest")

FetchContent_MakeAvailable(sonic-cpp)
