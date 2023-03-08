
#include <fstream>
#include <fmt/format.h>

int main() {
  constexpr static auto N{500};  // N^2 = 250,000 polygons
  {
    auto file = std::ofstream{"geometry.in"};

    for (auto i{0}; i < N; ++i) {
      for (auto j{0}; j < N; ++j) {
        file << fmt::format("POLYGON(({} {}, {} {}, {} {}, {} {}))\n", i, j, i + 5, j + 1, i + 1,
                            j + 5, i, j);
      }
    }
  }
  {
    auto file = std::ofstream{"geometry_more.in"};

    for (auto i{0}; i < N; ++i) {
      for (auto j{0}; j < N; ++j) {
        file << fmt::format("POLYGON(({} {}, {} {}, {} {}, {} {}))\n", i, j + 1, i + 5, j + 5,
                            i + 1, j, i, j + 1);
      }
    }
  }
}
