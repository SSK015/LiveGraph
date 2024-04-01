cmake -S . -B build -DCMAKE_BUILD_TYPE=Release  -DWITH_TRICACHE=OFF
cmake -S . -B build-cache -DCMAKE_BUILD_TYPE=Release -DWITH_TRICACHE=ON
cmake --build build -j
cmake --build build-cache -j
