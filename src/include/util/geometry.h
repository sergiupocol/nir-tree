// Copyright 2021 Kyle Langendoen

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef __GEOMETRY__
#define __GEOMETRY__

#include <iostream>
#include <algorithm>
#include <queue>
#include <cassert>
#include <vector>
#include <cmath>
#include <cstring>
#include <limits>
#include <cfenv>
#include <iterator>
#include <functional>
#include <util/debug.h>
#include <globals/globals.h>
#include <variant>

class Point
{
	public:

        double values[dimensions];

		static Point atInfinity;
		static Point atNegInfinity;
		static Point atOrigin;

		Point();

		Point(double x, double y);
		Point(double value);
		Point(const Point &o) = default;

		bool orderedCompare(const Point &rhs, unsigned startingDimension) const;
		double distance(const Point &p) const;

		Point &operator-=(const Point &rhs);
		Point &operator+=(const Point &rhs);
		Point &operator/=(double scalar);
		Point &operator*=(double scalar);
		Point &operator*=(const Point &rhs);
		double &operator[](unsigned index);
		const double operator[](unsigned index) const;
		Point &operator<<(const Point &p);
		Point &operator>>(const Point &p);

		friend Point operator-(const Point &lhs, const Point &rhs);
		friend Point operator+(const Point &lhs, const Point &rhs);
		friend Point operator*(const Point &lhs, const double scalar);
		friend Point operator/(const Point &lsh, const double scalar);
		friend Point operator*(const Point &lhs, const Point &rhs);
		friend bool operator<(const Point &lhs, const Point &rhs);
		friend bool operator>(const Point &lhs, const Point &rhs);
		friend bool operator<=(const Point &lhs, const Point &rhs);
		friend bool operator>=(const Point &lhs, const Point &rhs);
		friend bool operator==(const Point &lhs, const Point &rhs);
		friend bool operator!=(const Point &lhs, const Point &rhs);
		friend std::ostream& operator<<(std::ostream &os, const Point &point);

};

bool operator<(const Point &lhs, const Point &rhs);
bool operator>(const Point &lhs, const Point &rhs);
bool operator<=(const Point &lhs, const Point &rhs);
bool operator>=(const Point &lhs, const Point &rhs);
bool operator==(const Point &lhs, const Point &rhs);
bool operator!=(const Point &lhs, const Point &rhs);

class Rectangle
{
	public:
		static Rectangle atInfinity;
		static Rectangle atNegInfinity;
		static Rectangle atOrigin;

		Point lowerLeft;
		Point upperRight;

		Rectangle();
		Rectangle(double x, double y, double xp, double yp);
		Rectangle(Point lowerLeft, Point upperRight);
		Rectangle(const Rectangle &o) = default;
		double area() const;
		double margin() const;
		double computeIntersectionArea(const Rectangle &givenRectangle) const;
		double computeExpansionArea(const Point &givenPoint) const;
		double computeExpansionMargin(const Point &givenPoint) const;
		double computeExpansionArea(const Rectangle &givenRectangle) const;
		double marginDelta(const Point &givenPoint, const Rectangle &givenRectangle) const;
		double areaDelta(const Point &givenPoint, const Rectangle &givenRectangle) const;
		void expand(const Point &givenPoint);
		void expand(const Rectangle &givenRectangle);
		bool alignedForMerging(const Rectangle &givenRectangle) const;
		bool alignedOpposingBorders(const Rectangle &givenRectangle) const;
		bool intersectsRectangle(const Rectangle &givenRectangle) const;
		bool strictIntersectsRectangle(const Rectangle &givenRectangle) const;
		bool borderOnlyIntersectsRectangle(const Rectangle &givenRectangle) const;
		bool containsPoint(const Point &givenPoint) const;
		bool strictContainsPoint(const Point &givenPoint) const;
		bool containsRectangle(const Rectangle &givenRectangle) const;
		Point centrePoint() const;
		Rectangle copyExpand(const Point &givenPoint) const;
		Rectangle intersection(const Rectangle &clippingRectangle) const;
		std::vector<Rectangle> fragmentRectangle(const Rectangle &clippingRectangle) const;

		friend bool operator==(const Rectangle &lr, const Rectangle &rr);
		friend bool operator!=(const Rectangle &lr, const Rectangle &rr);
		friend std::ostream& operator<<(std::ostream& os, const Rectangle &rectangle);
};

bool operator==(const Rectangle &lhs, const Rectangle &rhs);
bool operator!=(const Rectangle &lhs, const Rectangle &rhs);

class IsotheticPolygon
{
	public:
		struct OptimalExpansion
		{
			unsigned index;
			double area;
		};

		Rectangle boundingBox;
		std::vector<Rectangle> basicRectangles;

		IsotheticPolygon();
		explicit IsotheticPolygon(const Rectangle &baseRectangle);
		IsotheticPolygon(const IsotheticPolygon &basePolygon);

		double area() const;
		double computeIntersectionArea(const Rectangle &givenRectangle) const;
		OptimalExpansion computeExpansionArea(const Point &givenPoint) const;
		OptimalExpansion computeExpansionArea(const Rectangle &givenRectangle) const;
		void expand(const Point &givenPoint);
		void expand(const Point &givenPoint, const OptimalExpansion &expansion);
		bool intersectsRectangle(const Rectangle &givenRectangle) const;
		bool intersectsPolygon(const IsotheticPolygon &givenPolygon) const;
		bool borderOnlyIntersectsRectangle(const Rectangle &givenRectangle) const;
		bool containsPoint(const Point &givenPoint) const ;
		bool disjoint(const IsotheticPolygon &givenPolygon) const;
		std::vector<Rectangle> intersection(const Rectangle &givenRectangle) const;
		void intersection(const IsotheticPolygon &constraintPolygon);
		void increaseResolution(const Point &givenPoint, const Rectangle &clippingRectangle);
		void increaseResolution(const Point &givenPoint, const IsotheticPolygon &clippingPolygon);
		void maxLimit(double limit, unsigned d=0);
		void minLimit(double limit, unsigned d=0);
		void merge(const IsotheticPolygon &mergePolygon);
		void remove(unsigned basicRectangleIndex);
		void deduplicate();
		void refine();
		void shrink(const std::vector<Point> &pinPoints);

		bool exists() const;
		bool valid() const;
		bool unique() const;
		bool lineFree() const;
		bool infFree() const;

		friend bool operator==(const IsotheticPolygon &lhs, const IsotheticPolygon &rhs);
		friend bool operator!=(const IsotheticPolygon &lhs, const IsotheticPolygon &rhs);
		friend std::ostream& operator<<(std::ostream &os, const IsotheticPolygon &polygon);
};

bool operator==(const IsotheticPolygon &lhs, const IsotheticPolygon &rhs);
bool operator!=(const IsotheticPolygon &lhs, const IsotheticPolygon &rhs);


#define MAX_RECTANGLE_COUNT 5 
class InlineBoundedIsotheticPolygon {
	public:
		struct OptimalExpansion
		{
			unsigned index;
			double area;
		};

        unsigned rectangle_count_;
		Rectangle boundingBox;
		std::array<Rectangle, MAX_RECTANGLE_COUNT> basicRectangles;

		InlineBoundedIsotheticPolygon();
		explicit InlineBoundedIsotheticPolygon(const Rectangle &baseRectangle);
		InlineBoundedIsotheticPolygon(const InlineBoundedIsotheticPolygon &basePolygon);

		double area() const;
		double computeIntersectionArea(const Rectangle &givenRectangle) const;
		OptimalExpansion computeExpansionArea(const Point &givenPoint) const;
		OptimalExpansion computeExpansionArea(const Rectangle &givenRectangle) const;
		void expand(const Point &givenPoint);
		void expand(const Point &givenPoint, const OptimalExpansion &expansion);
		bool intersectsRectangle(const Rectangle &givenRectangle) const;
		bool intersectsPolygon(const InlineBoundedIsotheticPolygon &givenPolygon) const;
		bool borderOnlyIntersectsRectangle(const Rectangle &givenRectangle) const;
		bool containsPoint(const Point &givenPoint) const ;
		bool disjoint(const InlineBoundedIsotheticPolygon &givenPolygon) const;
		std::vector<Rectangle> intersection(const Rectangle &givenRectangle) const;
		void intersection(const InlineBoundedIsotheticPolygon &constraintPolygon);
		void increaseResolution(const Point &givenPoint, const Rectangle &clippingRectangle);
		void increaseResolution(const Point &givenPoint, const InlineBoundedIsotheticPolygon &clippingPolygon);
		void maxLimit(double limit, unsigned d=0);
		void minLimit(double limit, unsigned d=0);
		void merge(const InlineBoundedIsotheticPolygon &mergePolygon);
		void remove(unsigned basicRectangleIndex);
		void deduplicate();
		void refine();
        
        template <class iterator_type>
        void shrink( iterator_type begin, iterator_type end )
        {
            // Early exit
            if( begin == end or rectangle_count_ == 0 ) {
                return;
            }

            std::vector<Rectangle> rectangleSetShrunk;
            for( unsigned i = 0; i < rectangle_count_; i++ ) {
                Rectangle &basicRectangle = basicRectangles[i];
                bool addRectangle = false;
                Rectangle shrunkRectangle = Rectangle(Point::atInfinity, Point::atNegInfinity);
                for( auto iter = begin; iter != end; iter++ ) {
                    Point &pinPoint = std::get<Point>( *iter );
                    if( basicRectangle.containsPoint(pinPoint) ) {
                        shrunkRectangle.expand(pinPoint);
                        addRectangle = true;
                        assert(shrunkRectangle.containsPoint(pinPoint));
                    }
                }

                if( addRectangle ) {
                    rectangleSetShrunk.emplace_back(std::move(shrunkRectangle));
                }
            }

            assert(rectangleSetShrunk.size() > 0);

            for( rectangle_count_ = 0; rectangle_count_ <
                    rectangleSetShrunk.size();
                    rectangle_count_++ ) {
                basicRectangles[ rectangle_count_ ] = rectangleSetShrunk[
                    rectangle_count_ ];
            }
        }

		bool exists() const;
		bool valid() const;
		bool unique() const;
		bool lineFree() const;
		bool infFree() const;

		friend bool operator==(const InlineBoundedIsotheticPolygon &lhs, const InlineBoundedIsotheticPolygon &rhs);
		friend bool operator!=(const InlineBoundedIsotheticPolygon &lhs, const InlineBoundedIsotheticPolygon &rhs);
		friend std::ostream& operator<<(std::ostream &os, const InlineBoundedIsotheticPolygon &polygon);

};

bool operator==(const InlineBoundedIsotheticPolygon &lhs, const InlineBoundedIsotheticPolygon &rhs);
bool operator!=(const InlineBoundedIsotheticPolygon &lhs, const InlineBoundedIsotheticPolygon &rhs);

// DO NOT MATERIALIZE ON THE STACK
class InlineUnboundedIsotheticPolygon {
	public:
		struct OptimalExpansion
		{
			unsigned index;
			double area;
		};

        unsigned rectangle_count_;
        unsigned max_rectangle_count_;
		Rectangle boundingBox;
        // Flexible array member
		Rectangle basicRectangles[1];

		InlineUnboundedIsotheticPolygon( unsigned max_rectangle_count )
            : max_rectangle_count_(max_rectangle_count) {}
		explicit InlineUnboundedIsotheticPolygon(const Rectangle &baseRectangle);
		InlineUnboundedIsotheticPolygon(const InlineUnboundedIsotheticPolygon &basePolygon);

		double area() const;
		double computeIntersectionArea(const Rectangle &givenRectangle) const;
		OptimalExpansion computeExpansionArea(const Point &givenPoint) const;
		OptimalExpansion computeExpansionArea(const Rectangle &givenRectangle) const;
		void expand(const Point &givenPoint);
		void expand(const Point &givenPoint, const OptimalExpansion &expansion);
		bool intersectsRectangle(const Rectangle &givenRectangle) const;
		bool intersectsPolygon(const InlineUnboundedIsotheticPolygon &givenPolygon) const;
		bool borderOnlyIntersectsRectangle(const Rectangle &givenRectangle) const;
		bool containsPoint(const Point &givenPoint) const ;
		bool disjoint(const InlineUnboundedIsotheticPolygon &givenPolygon) const;
		std::vector<Rectangle> intersection(const Rectangle &givenRectangle) const;
		void intersection(const InlineUnboundedIsotheticPolygon &constraintPolygon);
		void increaseResolution(const Point &givenPoint, const Rectangle &clippingRectangle);
		void increaseResolution(const Point &givenPoint, const InlineUnboundedIsotheticPolygon &clippingPolygon);
		void maxLimit(double limit, unsigned d=0);
		void minLimit(double limit, unsigned d=0);
		void merge(const InlineUnboundedIsotheticPolygon &mergePolygon);
		void remove(unsigned basicRectangleIndex);
		void deduplicate();
		void refine();
        
        template <class iterator_type>
        void shrink( iterator_type begin, iterator_type end )
        {
            // Early exit
            if( begin == end or rectangle_count_ == 0 ) {
                return;
            }

            std::vector<Rectangle> rectangleSetShrunk;
            for( unsigned i = 0; i < rectangle_count_; i++ ) {
                Rectangle &basicRectangle = basicRectangles[i];
                bool addRectangle = false;
                Rectangle shrunkRectangle = Rectangle(Point::atInfinity, Point::atNegInfinity);
                for( auto iter = begin; iter != end; iter++ ) {
                    Point &pinPoint = std::get<Point>( *iter );
                    if( basicRectangle.containsPoint(pinPoint) ) {
                        shrunkRectangle.expand(pinPoint);
                        addRectangle = true;
                        assert(shrunkRectangle.containsPoint(pinPoint));
                    }
                }

                if( addRectangle ) {
                    rectangleSetShrunk.emplace_back(std::move(shrunkRectangle));
                }
            }

            assert(rectangleSetShrunk.size() > 0);

            for( rectangle_count_ = 0; rectangle_count_ <
                    rectangleSetShrunk.size();
                    rectangle_count_++ ) {
                basicRectangles[ rectangle_count_ ] = rectangleSetShrunk[
                    rectangle_count_ ];
            }
        }

		bool exists() const;
		bool valid() const;
		bool unique() const;
		bool lineFree() const;
		bool infFree() const;

		friend bool operator==(const InlineUnboundedIsotheticPolygon
                &lhs, const InlineUnboundedIsotheticPolygon &rhs);
		friend bool operator!=(const InlineUnboundedIsotheticPolygon
                &lhs, const InlineUnboundedIsotheticPolygon &rhs);
		friend std::ostream& operator<<(std::ostream &os, const InlineUnboundedIsotheticPolygon &polygon);

};

bool operator==(const InlineUnboundedIsotheticPolygon &lhs, const
        InlineUnboundedIsotheticPolygon &rhs);
bool operator!=(const InlineUnboundedIsotheticPolygon &lhs, const
        InlineUnboundedIsotheticPolygon &rhs);


#endif
