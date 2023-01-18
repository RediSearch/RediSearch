
#include <cstdarg>
#include <ranges>
#include "polygon.hpp"


Polygon *Polygon_NewByCoords(int num_points, ...) {
	Polygon *poly = new Polygon{};
	std::va_list ap;
	va_start(ap, num_points);
	for ([[maybe_unused]] auto&& _ : std::views::iota(0, num_points)) {
		double x = va_arg(ap, double);
		double y = va_arg(ap, double);
		bg::append(poly->poly_.outer(), Point::point_internal{x, y});
	}
	va_end(ap);
	return poly;
}

Polygon *Polygon_NewByPoints(int num_points, ...) {
	Polygon *poly = new Polygon{};
	std::va_list ap;
	va_start(ap, num_points);
	for ([[maybe_unused]] auto&& _ : std::views::iota(0, num_points)) {
		Point *p = va_arg(ap, Point *);
		bg::append(poly->poly_.outer(), p->point_);
	}
	va_end(ap);
	return poly;
}

Polygon *Polygon_Copy(Polygon const *other) {
	return new Polygon{*other};
}

void Polygon_Free(Polygon *polygon) {
	delete polygon;
}

bool Polygon_IsEqual(Polygon const *lhs, Polygon const *rhs) {
	return *lhs == *rhs;
}

#include <iostream>
void Polygon_Print(Polygon const *poly) {
	std::cout << bg::wkt(poly->poly_) << "\n";
}
